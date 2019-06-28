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

package com.cognitree.kronos.queue;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.ControlMessage;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.model.TaskStatusUpdate;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.queue.producer.ProducerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class QueueService implements Service {
    public static final String EXECUTOR_QUEUE = "executor-queue";
    public static final String SCHEDULER_QUEUE = "scheduler-queue";

    private static final Logger logger = LoggerFactory.getLogger(QueueService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CONSUMER_KEY = "consumerKey";

    private final ConsumerConfig consumerConfig;
    private final ProducerConfig producerConfig;
    private final String taskStatusQueue;
    private final String controlQueue;

    private final ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Producer> producers = new ConcurrentHashMap<>();
    private String serviceName;

    public QueueService(QueueConfig queueConfig, String serviceName) {
        this.serviceName = serviceName;
        this.consumerConfig = queueConfig.getConsumerConfig();
        this.producerConfig = queueConfig.getProducerConfig();
        this.taskStatusQueue = queueConfig.getTaskStatusQueue();
        this.controlQueue = queueConfig.getControlMessageQueue();
    }

    public static QueueService getService(String serviceName) {
        return (QueueService) ServiceProvider.getService(serviceName);
    }

    @Override
    public void init() {
        logger.info("Initializing queue service {}", serviceName);
        ServiceProvider.registerService(this);
    }

    @Override
    public void start() {
        logger.info("Starting queue service {}", serviceName);
    }

    /**
     * Send task (not necessarily ordered)
     *
     * @param task
     * @throws ServiceException
     */
    public void send(Task task) throws ServiceException {
        logger.debug("Received request to send task {}", task.getIdentity());
        final String type = task.getType();
        if (!producers.containsKey(type)) {
            createProducer(type);
        }
        try {
            producers.get(type).send(MAPPER.writeValueAsString(task));
        } catch (IOException e) {
            logger.error("Error serializing task {}", task, e);
        }
    }

    /**
     * Send the task status in an ordered manner.
     *
     * @param taskStatusUpdate
     * @throws ServiceException
     */
    public void send(TaskStatusUpdate taskStatusUpdate) throws ServiceException {
        logger.debug("Received request to send task status update {}", taskStatusUpdate);
        if (!producers.containsKey(taskStatusQueue)) {
            createProducer(taskStatusQueue);
        }
        try {
            producers.get(taskStatusQueue).sendInOrder(MAPPER.writeValueAsString(taskStatusUpdate),
                    getOrderingKey(taskStatusUpdate.getTaskId()));
        } catch (IOException e) {
            logger.error("Error serializing task status update {}", taskStatusUpdate, e);
        }
    }

    private String getOrderingKey(TaskId taskId) {
        return taskId.getNamespace() + taskId.getWorkflow()
                + taskId.getJob() + taskId.getName();
    }

    /**
     * Broadcast control messages
     *
     * @param controlMessage
     * @throws ServiceException
     */
    public void send(ControlMessage controlMessage) throws ServiceException {
        logger.debug("Received request to send task control message {}", controlMessage);
        if (!producers.containsKey(controlQueue)) {
            createProducer(controlQueue);
        }
        try {
            producers.get(controlQueue).broadcast(MAPPER.writeValueAsString(controlMessage));
        } catch (IOException e) {
            logger.error("Error serializing control message {}", controlMessage, e);
        }
    }

    public List<Task> consumeTask(String type, int maxTasksToPoll) throws ServiceException {
        logger.debug("Received request to consume {} tasks of type {}", maxTasksToPoll, type);
        if (!consumers.containsKey(type)) {
            createConsumer(type, type);
        }
        final List<String> records = consumers.get(type).poll(maxTasksToPoll);
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        final ArrayList<Task> tasks = new ArrayList<>();
        for (String record : records) {
            try {
                tasks.add(MAPPER.readValue(record, Task.class));
            } catch (IOException e) {
                logger.error("Error parsing record {} to Task", record, e);
            }
        }
        return tasks;
    }

    public List<TaskStatusUpdate> consumeTaskStatusUpdates() throws ServiceException {
        logger.debug("Received request to consume task status update");
        if (!consumers.containsKey(taskStatusQueue)) {
            createConsumer(taskStatusQueue, taskStatusQueue);
        }
        final List<String> records = consumers.get(taskStatusQueue).poll();
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        final ArrayList<TaskStatusUpdate> taskStatusUpdates = new ArrayList<>();
        for (String record : records) {
            try {
                taskStatusUpdates.add(MAPPER.readValue(record, TaskStatusUpdate.class));
            } catch (IOException e) {
                logger.error("Error parsing record {} to TaskStatusUpdate", record, e);
            }
        }
        return taskStatusUpdates;
    }

    public List<ControlMessage> consumeControlMessages() throws ServiceException {
        logger.debug("Received request to consume control message");
        if (!consumers.containsKey(controlQueue)) {
            createConsumer(controlQueue, "controlMessage-" + UUID.randomUUID().toString());
        }
        final List<String> records = consumers.get(controlQueue).poll();
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        final ArrayList<ControlMessage> controlMessages = new ArrayList<>();
        for (String record : records) {
            try {
                controlMessages.add(MAPPER.readValue(record, ControlMessage.class));
            } catch (IOException e) {
                logger.error("Error parsing record {} to ControlMessage", record, e);
            }
        }
        return controlMessages;
    }

    private synchronized void createProducer(String topic) throws ServiceException {
        if (!producers.containsKey(topic)) {
            logger.info("Creating producer with for topic {}", topic);
            try {
                final Producer producer = (Producer) Class.forName(producerConfig.getProducerClass())
                        .getConstructor()
                        .newInstance();
                producer.init(topic, producerConfig.getConfig());
                producers.put(topic, producer);
            } catch (Exception e) {
                logger.error("Error creating producer for topic {}", topic, e);
                throw new ServiceException("Error creating producer for topic " + topic, e.getCause());
            }
        }
    }

    private synchronized void createConsumer(String topic, String consumerKey) throws ServiceException {
        if (!consumers.containsKey(topic)) {
            logger.info("Creating consumer for topic {} with consumer key {}", topic, consumerKey);
            try {
                final ObjectNode consumerConfig = this.consumerConfig.getConfig() == null ? MAPPER.createObjectNode()
                        : this.consumerConfig.getConfig().deepCopy();
                // uniqueness to identify consumers in clustered setup
                // a record should be consumed by only one consumer if they share the same consumer key
                consumerConfig.put(CONSUMER_KEY, consumerKey);
                final Consumer consumer = (Consumer) Class.forName(this.consumerConfig.getConsumerClass())
                        .getConstructor()
                        .newInstance();
                consumer.init(topic, consumerConfig);
                consumers.put(topic, consumer);
            } catch (Exception e) {
                logger.error("Error creating consumer for topic {}", topic, e);
                throw new ServiceException("Error creating consumer for topic " + topic, e.getCause());
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping queue service {}", serviceName);
        producers.forEach((s, producer) -> producer.close());
        consumers.forEach((s, consumer) -> consumer.close());
    }

    public void destroy() {
        logger.info("Destroying queue service {}", serviceName);
        consumers.forEach((s, consumer) -> consumer.destroy());
    }

    @Override
    public String getName() {
        return serviceName;
    }
}
