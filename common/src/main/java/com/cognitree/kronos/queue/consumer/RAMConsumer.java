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

import com.cognitree.kronos.queue.RAMQueueFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class RAMConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(RAMConsumer.class);

    @Override
    public void init(ObjectNode config) {
        logger.info("Initializing consumer for RAM(in-memory) queue with config {}", config);
    }
    @Override
    public void initTopic(String topic){}

    @Override
    public List<String> poll(String topic) {
        return poll(topic, Integer.MAX_VALUE);
    }

    @Override
    public List<String> poll(String topic, int size) {
        logger.trace("Received request to poll messages from topic {} with max size {}", topic, size);
        final LinkedBlockingQueue<String> blockingQueue = RAMQueueFactory.getQueue(topic);
        List<String> records = new ArrayList<>();
        while (!blockingQueue.isEmpty() && records.size() < size)
            records.add(blockingQueue.poll());
        return records;
    }

    @Override
    public void close() {

    }
}
