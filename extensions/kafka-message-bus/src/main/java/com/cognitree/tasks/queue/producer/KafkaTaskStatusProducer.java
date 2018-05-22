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

package com.cognitree.tasks.queue.producer;

import com.cognitree.tasks.model.TaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaTaskStatusProducer implements Producer<TaskStatus> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTaskStatusProducer.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TASK_STATUS_TOPIC = "taskStatus";

    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaTaskStatusProducer(ObjectNode config) {
        Properties producerConfig = OBJECT_MAPPER.convertValue(config.get("producerConfig"), Properties.class);
        kafkaProducer = new KafkaProducer<>(producerConfig);
    }

    @Override
    public void add(TaskStatus taskStatus) throws Exception {
        logger.info("Received request to add task status {} to queue", taskStatus);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(TASK_STATUS_TOPIC, TASK_STATUS_TOPIC, OBJECT_MAPPER.writeValueAsString(taskStatus));
        kafkaProducer.send(record);
    }

    @Override
    public void close() {
        // do nothing
    }
}