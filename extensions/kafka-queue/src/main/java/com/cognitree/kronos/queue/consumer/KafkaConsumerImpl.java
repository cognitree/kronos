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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link Consumer} implementation using Kafka as queue in backend.
 */
public class KafkaConsumerImpl implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Map<String, KafkaConsumer<String, String>> topicToKafkaConsumerMap = new HashMap<>();
    private Properties kafkaConsumerConfig;
    private long pollTimeoutInMs;


    public void init(ObjectNode config) {
        logger.info("Initializing consumer for kafka with config {}", config);
        kafkaConsumerConfig = OBJECT_MAPPER.convertValue(config.get("kafkaConsumerConfig"), Properties.class);
        // force override consumer configuration for kafka to poll max 1 message at a time
        kafkaConsumerConfig.put("max.poll.records", 1);
        pollTimeoutInMs = config.get("pollTimeoutInMs").asLong();
    }

    @Override
    public List<String> poll(String topic) {
        return poll(topic, Integer.MAX_VALUE);
    }

    @Override
    public List<String> poll(String topic, int size) {
        logger.trace("Received request to poll messages from topic {} with max size {}", topic, size);
        List<String> tasks = new ArrayList<>();
        if (!topicToKafkaConsumerMap.containsKey(topic)) {
            createKafkaConsumer(topic);
        }

        while (tasks.size() < size) {
            final ConsumerRecords<String, String> consumerRecords = topicToKafkaConsumerMap.get(topic).poll(pollTimeoutInMs);
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
        if (!topicToKafkaConsumerMap.containsKey(topic)) {
            logger.info("Creating kafka consumer on topic {} with consumer config {}", topic, kafkaConsumerConfig);
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConsumerConfig);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            topicToKafkaConsumerMap.put(topic, kafkaConsumer);
        }
    }

    @Override
    public void close() {
    }
}