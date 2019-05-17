/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.execution;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.TaskApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig$;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.SideInputsProcessor;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TestLocalTableDescriptor;
import org.apache.samza.testUtils.StreamTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestJobGraphStageCalculator {

  private static final String DEFAULT_SYSTEM = "test-system";
  private static final int DEFAULT_PARTITIONS = 10;

  private final Set<SystemDescriptor> systemDescriptors = new HashSet<>();
  private final Map<String, InputDescriptor> inputDescriptors = new HashMap<>();
  private final Map<String, OutputDescriptor> outputDescriptors = new HashMap<>();
  private final Set<TableDescriptor> tableDescriptors = new HashSet<>();

  private SystemAdmins systemAdmins;
  private StreamManager streamManager;
  private Config config;

  private StreamSpec input1Spec;
  private GenericInputDescriptor<KV<Object, Object>> input1Descriptor;
  private StreamSpec input2Spec;
  private GenericInputDescriptor<KV<Object, Object>> input2Descriptor;
  private StreamSpec input3Spec;
  private GenericInputDescriptor<KV<Object, Object>> input3Descriptor;
  private GenericInputDescriptor<KV<Object, Object>> input4Descriptor;
  private StreamSpec output1Spec;
  private GenericOutputDescriptor<KV<Object, Object>> output1Descriptor;
  private StreamSpec output2Spec;
  private GenericOutputDescriptor<KV<Object, Object>> output2Descriptor;
  private GenericSystemDescriptor system1Descriptor;
  private GenericSystemDescriptor system2Descriptor;

  static SystemAdmin createSystemAdmin(Map<String, Integer> streamToPartitions) {

    return new SystemAdmin() {
      @Override
      public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        return null;
      }

      @Override
      public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        Map<String, SystemStreamMetadata> map = new HashMap<>();
        for (String stream : streamNames) {
          Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> m = new HashMap<>();
          for (int i = 0; i < streamToPartitions.get(stream); i++) {
            m.put(new Partition(i), new SystemStreamMetadata.SystemStreamPartitionMetadata("", "", ""));
          }
          map.put(stream, new SystemStreamMetadata(stream, m));
        }
        return map;
      }

      @Override
      public Integer offsetComparator(String offset1, String offset2) {
        return null;
      }
    };
  }

  @Before
  public void setup() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), DEFAULT_SYSTEM);
    StreamTestUtils.addStreamConfigs(configMap, "input1", "system1", "input1");
    StreamTestUtils.addStreamConfigs(configMap, "input2", "system2", "input2");
    StreamTestUtils.addStreamConfigs(configMap, "input3", "system2", "input3");
    StreamTestUtils.addStreamConfigs(configMap, "input4", "system1", "input4");
    StreamTestUtils.addStreamConfigs(configMap, "output1", "system1", "output1");
    StreamTestUtils.addStreamConfigs(configMap, "output2", "system2", "output2");
    config = new MapConfig(configMap);

    input1Spec = new StreamSpec("input1", "input1", "system1");
    input2Spec = new StreamSpec("input2", "input2", "system2");
    input3Spec = new StreamSpec("input3", "input3", "system2");

    output1Spec = new StreamSpec("output1", "output1", "system1");
    output2Spec = new StreamSpec("output2", "output2", "system2");

    KVSerde<Object, Object> kvSerde = new KVSerde<>(new NoOpSerde(), new NoOpSerde());
    String mockSystemFactoryClass = "factory.class.name";
    system1Descriptor = new GenericSystemDescriptor("system1", mockSystemFactoryClass);
    system2Descriptor = new GenericSystemDescriptor("system2", mockSystemFactoryClass);
    input1Descriptor = system1Descriptor.getInputDescriptor("input1", kvSerde);
    input2Descriptor = system2Descriptor.getInputDescriptor("input2", kvSerde);
    input3Descriptor = system2Descriptor.getInputDescriptor("input3", kvSerde);
    input4Descriptor = system1Descriptor.getInputDescriptor("input4", kvSerde);
    output1Descriptor = system1Descriptor.getOutputDescriptor("output1", kvSerde);
    output2Descriptor = system2Descriptor.getOutputDescriptor("output2", kvSerde);

    // clean and set up sets and maps of descriptors
    systemDescriptors.clear();
    inputDescriptors.clear();
    outputDescriptors.clear();
    tableDescriptors.clear();
    systemDescriptors.add(system1Descriptor);
    systemDescriptors.add(system2Descriptor);
    inputDescriptors.put(input1Descriptor.getStreamId(), input1Descriptor);
    inputDescriptors.put(input2Descriptor.getStreamId(), input2Descriptor);
    inputDescriptors.put(input3Descriptor.getStreamId(), input3Descriptor);
    inputDescriptors.put(input4Descriptor.getStreamId(), input4Descriptor);
    outputDescriptors.put(output1Descriptor.getStreamId(), output1Descriptor);
    outputDescriptors.put(output2Descriptor.getStreamId(), output2Descriptor);


    // set up external partition count
    Map<String, Integer> system1Map = new HashMap<>();
    system1Map.put("input1", 64);
    system1Map.put("output1", 64);
    system1Map.put("input4", 64);
    Map<String, Integer> system2Map = new HashMap<>();
    system2Map.put("input2", 64);
    system2Map.put("input3", 64);
    system2Map.put("output2", 64);

    SystemAdmin systemAdmin1 = createSystemAdmin(system1Map);
    SystemAdmin systemAdmin2 = createSystemAdmin(system2Map);
    systemAdmins = mock(SystemAdmins.class);
    when(systemAdmins.getSystemAdmin("system1")).thenReturn(systemAdmin1);
    when(systemAdmins.getSystemAdmin("system2")).thenReturn(systemAdmin2);
    streamManager = new StreamManager(systemAdmins);
  }

  @Test
  public void testMYCreateProcessorGraph() {
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    //StreamApplicationDescriptorImpl graphSpec = createMYStreamGraph2();
    //StreamApplicationDescriptorImpl graphSpec = createMYSimpleStreamGraph4();
    StreamApplicationDescriptorImpl graphSpec =  createMYStreamGraph4Dia();
    //StreamApplicationDescriptorImpl graphSpec =  createMYSimpleStreamTableJoin();
    //StreamApplicationDescriptorImpl graphSpec =  createMY1Ip2PB();

    JobGraph jobGraph = (JobGraph) planner.plan(graphSpec);

    JobGraphStageCalculator stageCalculator = new JobGraphStageCalculator(jobGraph);
    System.out.println("StreamId to Stage : " + stageCalculator.getStreamIdToStage().toString());
    System.out.println("Are the stages valid? : " + stageCalculator.validateStages());
  }

  private StreamApplicationDescriptorImpl createMYStreamGraph() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
      MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);
      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

      TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()));
      Table table = appDesc.getTable(tableDescriptor);

      messageStream2
          .map(kv->kv)
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
          .filter(m->true)
          .sendTo(table);

      messageStream1
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2")
          .join(table, mock(StreamTableJoinFunction.class))
          .filter(m->true)
          .join(messageStream3,
              mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
          .sendTo(output1);
    }, config);
  }

  private StreamApplicationDescriptorImpl createMYStreamGraph2() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
      MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);
      MessageStream<KV<Object, Object>> messageStreamInt;
      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

      TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()));
      Table table = appDesc.getTable(tableDescriptor);

      messageStream2
          .map(kv->kv)
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
          .filter(m->true)
          .sendTo(table);

      messageStreamInt = messageStream1
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2")
          .join(messageStream3,
              mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2");
      messageStreamInt
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p3")
          .join(table, mock(StreamTableJoinFunction.class))
          .sendTo(output1);
    }, config);
  }

  private StreamApplicationDescriptorImpl createMYStreamGraph3Pr() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
      MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);
      MessageStream<KV<Object, Object>> messageStreamInt1;
      MessageStream<KV<Object, Object>> messageStreamInt2;
      MessageStream<KV<Object, Object>> messageStreamInt3;
      MessageStream<KV<Object, Object>> messageStreamInt4;
      MessageStream<KV<Object, Object>> messageStreamInt5;
      MessageStream<KV<Object, Object>> messageStreamJ1;
      MessageStream<KV<Object, Object>> messageStreamJ2;

      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

      TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()));
      Table table = appDesc.getTable(tableDescriptor);

      messageStreamInt1 = messageStream1.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1");
      messageStreamInt2 = messageStream2.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2");

      messageStream3.sendTo(table);

      messageStreamJ1 = messageStreamInt1
          .join(messageStreamInt2,
              mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j1");

      messageStreamInt3 = messageStreamJ1.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p3");

      messageStreamJ2 = messageStreamInt3.join(table, mock(StreamTableJoinFunction.class));

      messageStreamInt4 = messageStreamJ2.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p4");

      messageStreamInt4.sendTo(output1);

      messageStreamInt5 = messageStream3.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p5");
      messageStreamInt5.sendTo(output1);
    }, config);
  }
  private StreamApplicationDescriptorImpl createMYStreamGraph4Dia() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
      MessageStream<KV<Object, Object>> messageStreamInt1;
      MessageStream<KV<Object, Object>> messageStreamJ1;

      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);


      messageStreamInt1 = messageStream1.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1");

      messageStreamJ1 = messageStream1
          .join(messageStream2,
              mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j1");

      messageStreamInt1
          .join(messageStreamJ1,
              mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
          .sendTo(output1);
    }, config);
  }

  private StreamApplicationDescriptorImpl createMYSimpleStreamGraph4() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream2 = appDesc.getInputStream(input2Descriptor);
      MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);

      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

      TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()));
      Table table = appDesc.getTable(tableDescriptor);

      messageStream3
          .sendTo(table);

      messageStream1
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2")
          .join(table, mock(StreamTableJoinFunction.class))
          .sendTo(output1);
    }, config);
  }


  private StreamApplicationDescriptorImpl createMYSimpleStreamTableJoin() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);

      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);

      TableDescriptor tableDescriptor = new TestLocalTableDescriptor.MockLocalTableDescriptor(
          "table-id", new KVSerde(new StringSerde(), new StringSerde()));
      Table table = appDesc.getTable(tableDescriptor);

      messageStream3
          .sendTo(table);

      messageStream1
          .join(table, mock(StreamTableJoinFunction.class))
          .sendTo(output1);
    }, config);
  }

  private StreamApplicationDescriptorImpl createMYSimpleStreamGraph1() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);


      messageStream1
          .filter(m->true)
          .sendTo(output1);
    }, config);
  }

  private StreamApplicationDescriptorImpl createMYSimpleStreamGraph2() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);


      messageStream1
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
          .filter(m->true)
          .sendTo(output1);
    }, config);
  }

  private StreamApplicationDescriptorImpl createMYSimpleStreamGraph3() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream3 = appDesc.getInputStream(input3Descriptor);
      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);


      messageStream1
          .partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1")
          .join(messageStream3,
              mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
          .sendTo(output1);
    }, config);
  }

  private StreamApplicationDescriptorImpl createMY1Ip2PB() {

    return new StreamApplicationDescriptorImpl(appDesc -> {
      MessageStream<KV<Object, Object>> messageStream = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<Object, Object>> messageStream1;
      MessageStream<KV<Object, Object>> messageStream2;
      MessageStream<KV<Object, Object>> messageStream3;
      OutputStream<KV<Object, Object>> output1 = appDesc.getOutputStream(output1Descriptor);



      messageStream2 = messageStream.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p2");
      messageStream3 = messageStream2.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p3");
      messageStream1 = messageStream.partitionBy(m -> m.key, m -> m.value, mock(KVSerde.class), "p1");

      messageStream1
          .join(messageStream3,
              mock(JoinFunction.class), mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
          .sendTo(output1);
    }, config);
  }
}
