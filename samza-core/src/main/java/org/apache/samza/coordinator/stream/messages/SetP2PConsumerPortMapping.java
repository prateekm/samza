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

package org.apache.samza.coordinator.stream.messages;

/**
 * SetP2PConsumerPortMapping is used internally by the Samza framework to
 * persist the p2p consumer port.
 *
 * Structure of the message looks like:
 * {
 *     Key: $ContainerId
 *     Type: set-p2p-consumer-port
 *     Source: "SamzaContainer-$ContainerId"
 *     MessageMap:
 *     {
 *         port: p2p consumer port
 *     }
 * }
 * */
public class SetP2PConsumerPortMapping extends CoordinatorStreamMessage {
  public static final String TYPE = "set-p2p-consumer-port";
  public static final String PORT_KEY = "port";

  public SetP2PConsumerPortMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * SetP2PConsumerPortMapping is used for the p2p consumer port information.
   * @param source the source of the message
   * @param key container id
   * @param port p2p consumer port
   */
  public SetP2PConsumerPortMapping(String source, String key, String port) {
    super(source);
    setType(TYPE);
    setKey(key);
    putMessageValue(PORT_KEY, port);
  }

  public String getPort() {
    return getMessageValue(PORT_KEY);
  }
}
