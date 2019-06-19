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

package com.cognitree.kronos.queue.consumer;


import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public abstract class Consumer {

    final String topic;
    final ObjectNode consumerConfig;

    /**
     * during initialization phase a call is made to initialize consumer using {@link ConsumerConfig#getConfig()}.
     * Any property required by the consumer¸ to instantiate itself should be part of {@link ConsumerConfig#getConfig()}.
     *
     * @param consumerConfig configuration used to initialize the consumer.
     */
    public Consumer(String topic, ObjectNode consumerConfig) {
        this.topic = topic;
        this.consumerConfig = consumerConfig;
    }

    /**
     * polls data from the underlying queue
     *
     * @return
     */
    public abstract List<String> poll();

    /**
     * polls data from the underlying queue
     *
     * @param maxSize maximum number of records to poll
     * @return
     */
    public abstract List<String> poll(int maxSize);

    public abstract void close();

    /**
     * deletes all the created consumers and topic from the underlying queue
     */
    public void destroy() {
        // do nothing by default
    }
}
