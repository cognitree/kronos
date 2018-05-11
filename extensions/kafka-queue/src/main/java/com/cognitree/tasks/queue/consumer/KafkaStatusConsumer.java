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
import com.cognitree.tasks.queue.Subscriber;
import com.cognitree.tasks.util.DateTimeUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class KafkaStatusConsumer implements Consumer<TaskStatus> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStatusConsumer.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TASK_STATUS_TOPIC = "taskStatus";
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final long pollTimeout;
    private final long pollInterval;

    private final KafkaConsumer<String, String> kafkaConsumer;

    public KafkaStatusConsumer(ObjectNode config) {
        Properties consumerConfig = OBJECT_MAPPER.convertValue(config.get("consumerConfig"), Properties.class);
        kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        kafkaConsumer.subscribe(Collections.singleton(TASK_STATUS_TOPIC));

        pollTimeout = DateTimeUtil.resolveDuration(config.get("pollTimeout").asText());
        pollInterval = DateTimeUtil.resolveDuration(config.get("pollInterval").asText());
    }

    @Override
    public void subscribe(Subscriber<TaskStatus> subscriber) {
        final Runnable taskConsumer = () -> {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(pollTimeout);
            if (!consumerRecords.isEmpty()) {
                List<TaskStatus> tasks = new ArrayList<>();
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    try {
                        tasks.add(OBJECT_MAPPER.readValue(consumerRecord.value(), TaskStatus.class));
                    } catch (IOException e) {
                        logger.error("Error parsing consumer record key {}, value {}",
                                consumerRecord.key(), consumerRecord.value(), e);
                    }
                }
                subscriber.consume(tasks);
            }
        };
        scheduledExecutorService.scheduleAtFixedRate(taskConsumer, pollInterval, pollInterval, MILLISECONDS);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(1, MINUTES);
        } catch (Exception e) {
            logger.error("Error stopping kafka task status consumer", e);
        }
    }
}