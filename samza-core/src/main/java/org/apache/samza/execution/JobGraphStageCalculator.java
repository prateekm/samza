package org.apache.samza.execution;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartitionByOperatorSpec;
import org.apache.samza.operators.spec.SendToTableOperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;


class JobGraphStageCalculator {
  private Collection<InputOperatorSpec> inputOpSpecs;
  private  Multimap<OperatorSpec, OperatorSpec> opSpecToPredecessors  = HashMultimap.create();
  private HashMap<String, InputOperatorSpec> streamidToInputOpSpec  = new HashMap<>();
  private Multimap<OperatorSpec, InputOperatorSpec> joinOpSpecToPredInputOpSpecs;
  private HashSet<OperatorSpec> sinks = new HashSet<>();
  private HashMap<String, Integer> streamIdToStage = new HashMap<>();

  public JobGraphStageCalculator (JobGraph jobGraph) {
    inputOpSpecs = jobGraph.getApplicationDescriptorImpl().getInputOperators().values();
    for (InputOperatorSpec inputOpSpec : inputOpSpecs) {
      streamidToInputOpSpec.put(inputOpSpec.getStreamId(), inputOpSpec);
    }

    joinOpSpecToPredInputOpSpecs =
        OperatorSpecGraphAnalyzer.getJoinToInputOperatorSpecs(inputOpSpecs);

    for (InputOperatorSpec inputOpSpec : inputOpSpecs) {
      computePredecessorsAndSinks(inputOpSpec);
    }

    labelStages();
  }

  public HashMap<String, Integer> getStreamIdToStage() {
    return streamIdToStage;
  }

  private void computePredecessorsAndSinks(OperatorSpec opSpec) {
    Collection<OperatorSpec> nextOpSpecs = opSpec.getRegisteredOperatorSpecs();

    if (opSpec instanceof PartitionByOperatorSpec) {
      PartitionByOperatorSpec pBSpec = ((PartitionByOperatorSpec) opSpec);
      nextOpSpecs.add(streamidToInputOpSpec.get(pBSpec.getOutputStream().getStreamId()));
    }

    if (opSpec instanceof JoinOperatorSpec || opSpec instanceof StreamTableJoinOperatorSpec) {
      joinOpSpecToPredInputOpSpecs.get(opSpec).forEach(spec -> {
        opSpecToPredecessors.put(opSpec, spec);
      });
    }
    if (nextOpSpecs.size() == 0) {
      sinks.add(opSpec);
    }
    nextOpSpecs.forEach(spec -> {
      opSpecToPredecessors.put(spec, opSpec);
      computePredecessorsAndSinks(spec);
    });
  }

  private void labelStages() {
    Queue<Pair<OperatorSpec,Integer>> queue = new LinkedList<>();

    sinks.forEach(sink -> {((LinkedList<Pair<OperatorSpec, Integer>>) queue).add(Pair.of(sink,1));});

    while (!queue.isEmpty()) {
      Pair<OperatorSpec, Integer> cur = queue.poll();
      OperatorSpec opSpec = cur.getLeft();
      Integer label = cur.getRight();
      if(opSpec instanceof InputOperatorSpec) {
        String streamId = ((InputOperatorSpec) opSpec).getStreamId();
        Integer currentLabel = streamIdToStage.get(streamId);
        if( currentLabel == null || currentLabel < label) {
          streamIdToStage.put(streamId, label);
        }
      }
      if(opSpec instanceof PartitionByOperatorSpec) {
        opSpecToPredecessors.get(opSpec).forEach(pred -> {
          ((LinkedList<Pair<OperatorSpec, Integer>>) queue).add(Pair.of(pred, label + 1));
        });
      } else {
        opSpecToPredecessors.get(opSpec).forEach(pred -> {
          ((LinkedList<Pair<OperatorSpec, Integer>>) queue).add(Pair.of(pred, label));
        });
      }
    }
  }

  public boolean validateStages() {
    if (streamIdToStage.size() == 0) {
      return true;
    }
    return validateParitionByStages() && validateJoinStages();
  }

  private InputOperatorSpec findInputStream(OperatorSpec opSpec) {
    OperatorSpec currentPredecessor = opSpec;
    while (! (currentPredecessor instanceof InputOperatorSpec)) {
      Collection<OperatorSpec> nextPredecessors = opSpecToPredecessors.get(currentPredecessor);
      if (nextPredecessors.size() == 1) {
        currentPredecessor = nextPredecessors.stream().findFirst().orElse(null);
      } else { // is a join operator
        currentPredecessor = joinOpSpecToPredInputOpSpecs.get(currentPredecessor).stream().findFirst().orElse(null);
      }
    }
    return (InputOperatorSpec) currentPredecessor;
  }

  private boolean validateParitionByStages() {
    HashMap<OperatorSpec, Pair<OperatorSpec, OperatorSpec>> partitionbyToIOStreams = new HashMap<>();
    HashMap<OperatorSpec, OperatorSpec> paritionbyToOutputStreams = new HashMap<>();

    opSpecToPredecessors.forEach((opSpec, predecessor) -> {
      if (predecessor instanceof PartitionByOperatorSpec) {
        paritionbyToOutputStreams.put(predecessor, opSpec);
      }
    });
    opSpecToPredecessors.forEach((opSpec, predecessor) -> {
      if(opSpec instanceof PartitionByOperatorSpec) {
        InputOperatorSpec inputStream = findInputStream(predecessor);
        if (inputStream != null) {
          partitionbyToIOStreams.put(opSpec, Pair.of(inputStream, paritionbyToOutputStreams.get(opSpec)));
        }
      }
    });

    for (Map.Entry<OperatorSpec, Pair<OperatorSpec, OperatorSpec>> entry: partitionbyToIOStreams.entrySet()) {
      Pair<OperatorSpec, OperatorSpec> ioPair = entry.getValue();
      String inputStreamId = ((InputOperatorSpec) ioPair.getLeft()).getStreamId();
      String outputStreamId = ((InputOperatorSpec) ioPair.getRight()).getStreamId();
      if (streamIdToStage.get(inputStreamId) <= streamIdToStage.get(outputStreamId)) {
        System.out.print("PartitionBy stage check failed for " + entry.getKey().getOpId());
        return false;
      }
    }
    return true;
  }

  private boolean validateJoinStages() {
    if (joinOpSpecToPredInputOpSpecs.size() == 0) {
      return true;
    }

    HashMap<OperatorSpec, Collection<InputOperatorSpec>> joinOpToInputCollection = new HashMap<>();

    for (OperatorSpec joinOp: joinOpSpecToPredInputOpSpecs.keySet()) {
      Collection<InputOperatorSpec> inputsToJoin = joinOpSpecToPredInputOpSpecs.get(joinOp);
      int prevStage = -1;
      for (InputOperatorSpec inputOperatorSpec: inputsToJoin) {
        int curStage = streamIdToStage.get(inputOperatorSpec.getStreamId());
        if(prevStage != -1 && (curStage != prevStage)) {
          System.out.println("Join stage check failed for Join op " + joinOp.getOpId());
          return false;
        }
        prevStage = curStage;
      }
    }
    return true;
  }

}
