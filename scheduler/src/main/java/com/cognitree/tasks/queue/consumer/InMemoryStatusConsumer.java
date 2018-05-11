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

package com.cognitree.tasks.queue.consumer;

import com.cognitree.tasks.model.TaskStatus;
import com.cognitree.tasks.queue.InMemoryQueueFactory;
import com.cognitree.tasks.queue.Subscriber;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static com.cognitree.tasks.util.DateTimeUtil.resolveDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class InMemoryStatusConsumer implements Consumer<TaskStatus> {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryStatusConsumer.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final LinkedBlockingQueue<TaskStatus> buffer;
    private final long pollInterval;

    public InMemoryStatusConsumer(ObjectNode config) {
        buffer = InMemoryQueueFactory.getBuffer(TaskStatus.class);
        this.pollInterval = resolveDuration(config.get("pollInterval").asText());
    }

    @Override
    public void subscribe(Subscriber<TaskStatus> subscriber) {
        final Runnable taskConsumer = () -> {
            if (buffer.isEmpty()) return;

            List<TaskStatus> tasksStatus = new ArrayList<>();
            buffer.drainTo(tasksStatus);
            subscriber.consume(tasksStatus);
        };
        scheduledExecutorService.scheduleAtFixedRate(taskConsumer, pollInterval, pollInterval, MILLISECONDS);
    }

    @Override
    public void close() {
        logger.info("stopping in memory task status consumer");
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(1, MINUTES);
        } catch (Exception e) {
            logger.error("Error stopping in memory task status consumer", e);
        }
    }
}
