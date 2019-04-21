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

package org.apache.samza.test.operator;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.test.operator.data.PageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionApp implements StreamApplication {
  private static final Logger LOG = LoggerFactory.getLogger(RepartitionApp.class);
  static final String SYSTEM = "kafka";
  static final String INPUT_TOPIC = "page-views";


  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KVSerde<String, PageView> inputSerde = KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>(PageView.class));
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(SYSTEM);
    KafkaInputDescriptor<KV<String, PageView>> id = ksd.getInputDescriptor(INPUT_TOPIC, inputSerde);

    appDescriptor.getInputStream(id)
        .map(kv -> {
            LOG.info("Received input message with key: {} value: {}", kv.key, kv.value);
            return kv.getValue();
          })
        .partitionBy(PageView::getUserId, m -> m, inputSerde, "p1")
        .map(kv -> {
            LOG.info("Received repartitioned message with key: {} value: {}", kv.key, kv.value);
            return kv;
          });
  }
}
