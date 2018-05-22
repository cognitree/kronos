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

import com.cognitree.kronos.model.TaskStatus;
import com.cognitree.kronos.queue.InMemoryQueueFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryStatusProducer implements Producer<TaskStatus> {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryStatusProducer.class);

    private final LinkedBlockingQueue<TaskStatus> buffer;

    public InMemoryStatusProducer(ObjectNode config) {
        buffer = InMemoryQueueFactory.getBuffer(TaskStatus.class);
    }

    @Override
    public void add(TaskStatus taskStatus) {
        logger.debug("Received request to add task status {} to queue", taskStatus);
        buffer.add(taskStatus);
    }

    @Override
    public void close() {
        // do nothing
    }
}
