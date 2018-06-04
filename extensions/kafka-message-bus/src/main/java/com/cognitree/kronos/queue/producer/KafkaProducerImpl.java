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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A {@link Producer} implementation using Kafka as queue in backend.
 */
public class KafkaProducerImpl implements Producer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KafkaProducer<String, String> kafkaProducer;

    public void init(ObjectNode config) {
        logger.info("Initializing producer for kafka with config {}", config);
        Properties producerConfig = OBJECT_MAPPER.convertValue(config.get("producerConfig"), Properties.class);
        kafkaProducer = new KafkaProducer<>(producerConfig);
    }

    @Override
    public void send(String topic, String record) {
        logger.trace("Received request to send message {} to topic {}.", record, topic);
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, record);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending record {} over kafka to topic {}.", record, topic, exception);
            }
        });
    }

    @Override
    public void close() {
    }
}