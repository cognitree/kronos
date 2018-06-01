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

import com.cognitree.kronos.util.DateTimeUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaConsumerImpl implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Map<String, KafkaConsumer<String, String>> kafkaConsumers = new HashMap<>();
    private Properties consumerConfig;
    private long pollTimeout;


    public void init(ObjectNode config) {
        logger.info("Initializing consumer for kafka with config {}", config);
        consumerConfig = OBJECT_MAPPER.convertValue(config.get("consumerConfig"), Properties.class);
        // force override consumer configuration for kafka to poll max 1 message at a time
        consumerConfig.put("max.poll.records", 1);
        pollTimeout = DateTimeUtil.resolveDuration(config.get("pollTimeout").asText());
    }

    @Override
    public List<String> poll(String topic) {
        return poll(topic, Integer.MAX_VALUE);
    }

    @Override
    public List<String> poll(String topic, int size) {
        logger.trace("Received request to poll messages from topic {} with max size {}", topic, size);
        List<String> tasks = new ArrayList<>();
        if (!kafkaConsumers.containsKey(topic)) {
            createKafkaConsumer(topic);
        }

        while (tasks.size() < size) {
            final ConsumerRecords<String, String> consumerRecords = kafkaConsumers.get(topic).poll(pollTimeout);
            if (consumerRecords.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                tasks.add(consumerRecord.value());
            }
        }

        return tasks;
    }

    private synchronized void createKafkaConsumer(String topic) {
        if (!kafkaConsumers.containsKey(topic)) {
            logger.info("Creating kafka consumer on topic {} with consumer config {}", topic, consumerConfig);
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            kafkaConsumers.put(topic, kafkaConsumer);
        }
    }

    @Override
    public void close() {
    }
}