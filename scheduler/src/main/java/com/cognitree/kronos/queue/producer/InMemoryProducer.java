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

import com.cognitree.kronos.queue.InMemoryQueueFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryProducer implements Producer {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryProducer.class);

    public void init(ObjectNode config) {
        logger.info("Initializing producer for in-memory queue with config", config);
    }

    @Override
    public void send(String topic, String record) {
        logger.trace("Received request to send message {} on topic {}", record, topic);
        InMemoryQueueFactory.getQueue(topic).add(record);
    }

    @Override
    public void close() {
        // do nothing
    }
}
