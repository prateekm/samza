package org.apache.samza.test.framework;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestStreamApplicationIntegrationTestHarness extends StreamApplicationIntegrationTestHarness {
  private static final String INPUT_TOPIC = "input";

  @Test
  public void testTheTestHarness() {
    List<String> inputMessages = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    // create input topic and produce the first batch of input messages
    boolean topicCreated = createTopic(INPUT_TOPIC, 1);
    if (!topicCreated) {
      fail("Could not create input topic.");
    }
    inputMessages.forEach(m -> produceMessage(INPUT_TOPIC, 0, m, m));

    // verify that the input messages were produced successfully
    if (inputMessages.size() > 0) {
      List<ConsumerRecord<String, String>> inputRecords =
          consumeMessages(Collections.singletonList(INPUT_TOPIC), inputMessages.size());
      List<String> readInputMessages = inputRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
      Assert.assertEquals(inputMessages, readInputMessages);
    }
  }
}
