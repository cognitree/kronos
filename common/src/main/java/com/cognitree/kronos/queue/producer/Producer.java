/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.queue.producer;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Producer {

    /**
     * during initialization phase a call is made to initialize producer using {@link ProducerConfig#getConfig()}.
     * Any property required by the producer¸ to instantiate itself should be part of {@link ProducerConfig#getConfig()}.
     *
     * @param topic  topic to create the producer
     * @param config configuration used to initialize the producer.
     */
    void init(String topic, ObjectNode config);

    void broadcast(String record);

    /**
     * sends the record to the underlying queue.
     * Records can be consumed out of order by the Consumer.
     *
     * @param record record to send
     */
    void send(String record);

    /**
     * sends the record to the underlying queue in-order.
     * Records should be consumed in-order by the Consumer.
     *
     * @param record      record to send
     * @param orderingKey key to decide how the message is sent for in-order delivery
     */
    void sendInOrder(String record, String orderingKey);

    void close();
}
