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

package com.cognitree.kronos.executor;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskStatus;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.queue.producer.ProducerConfig;
import com.cognitree.kronos.util.DateTimeUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.cognitree.kronos.model.Messages.MISSING_HANDLER;
import static com.cognitree.kronos.model.Task.Status.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A task execution service is responsible for initializing each {@link TaskHandler} and periodically polling new tasks
 * from queue and submitting it to appropriate handler for execution.
 * <p>
 * A task execution service acts as an consumer of tasks from queue and producer of task status to the queue.
 * </p>
 */
public final class TaskExecutionService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionService.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConsumerConfig consumerConfig;
    private final ProducerConfig producerConfig;
    private final String statusQueue;
    private final Map<String, TaskHandlerConfig> taskTypeToHandlerConfig;

    private final Map<String, TaskHandler> taskTypeToHandlerMap = new HashMap<>();
    private final Map<String, Integer> taskTypeToMaxParallelTasksCount = new HashMap<>();
    private final Map<String, Integer> taskTypeToRunningTasksCount = new HashMap<>();
    // used by internal tasks like polling new tasks from queue
    private final ScheduledExecutorService internalExecutorService = Executors.newSingleThreadScheduledExecutor();
    // used to execute tasks
    private final ExecutorService taskExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private Consumer consumer;
    private Producer producer;

    public TaskExecutionService(ExecutorConfig executorConfig, QueueConfig queueConfig) {
        this.consumerConfig = queueConfig.getConsumerConfig();
        this.producerConfig = queueConfig.getProducerConfig();
        this.statusQueue = queueConfig.getTaskStatusQueue();
        this.taskTypeToHandlerConfig = executorConfig.getTaskHandlerConfig();
    }

    public static TaskExecutionService getService() {
        return (TaskExecutionService) ServiceProvider.getService(TaskExecutionService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        initConsumer();
        initProducer();
        initTaskHandlersAndExecutors();
    }

    private void initConsumer() throws Exception {
        logger.info("Initializing consumer with config {}", consumerConfig);
        consumer = (Consumer) Class.forName(consumerConfig.getConsumerClass())
                .getConstructor()
                .newInstance();
        consumer.init(consumerConfig.getConfig());
    }

    private void initProducer() throws Exception {
        logger.info("Initializing producer with config {}", producerConfig);
        producer = (Producer) Class.forName(producerConfig.getProducerClass())
                .getConstructor()
                .newInstance();
        producer.init(producerConfig.getConfig());
    }

    private void initTaskHandlersAndExecutors() throws Exception {
        for (Map.Entry<String, TaskHandlerConfig> taskTypeToHandlerConfigEntry : taskTypeToHandlerConfig.entrySet()) {
            final String taskType = taskTypeToHandlerConfigEntry.getKey();
            final TaskHandlerConfig taskHandlerConfig = taskTypeToHandlerConfigEntry.getValue();
            logger.info("Initializing task handler of type {} with config {}", taskType, taskHandlerConfig);
            final TaskHandler taskHandler = (TaskHandler) Class.forName(taskHandlerConfig.getHandlerClass())
                    .newInstance();
            taskHandler.init(taskHandlerConfig.getConfig());
            taskTypeToHandlerMap.put(taskType, taskHandler);

            int maxParallelTasks = taskHandlerConfig.getMaxParallelTasks();
            maxParallelTasks = maxParallelTasks > 0 ? maxParallelTasks : Runtime.getRuntime().availableProcessors();
            taskTypeToMaxParallelTasksCount.put(taskType, maxParallelTasks);
            taskTypeToRunningTasksCount.put(taskType, 0);
        }
    }

    @Override
    public void start() {
        final long pollInterval = DateTimeUtil.resolveDuration(consumerConfig.getPollInterval());
        internalExecutorService.scheduleAtFixedRate(this::consumeTasks, 0, pollInterval, MILLISECONDS);
    }

    private void consumeTasks() {
        taskTypeToMaxParallelTasksCount.forEach((taskType, maxParallelTasks) -> {
            synchronized (taskTypeToRunningTasksCount) {
                final int tasksToPoll = maxParallelTasks - taskTypeToRunningTasksCount.get(taskType);
                if (tasksToPoll > 0) {
                    final List<String> tasks = consumer.poll(taskType, tasksToPoll);
                    for (String taskAsString : tasks) {
                        try {
                            submit(MAPPER.readValue(taskAsString, Task.class));
                        } catch (IOException e) {
                            logger.error("Error parsing task message {}", taskAsString, e);
                        }
                    }
                }
            }
        });
    }

    /**
     * submit the task for execution to appropriate handler based on task type.
     *
     * @param task task to submit for execution
     */
    private void submit(Task task) {
        logger.trace("Received task {} for execution from task queue", task);
        final TaskHandler handler = taskTypeToHandlerMap.get(task.getType());
        if (handler == null) {
            logger.error("No handler found to execute task {} of type {}, skipping task and marking it as {}",
                    task, task.getType(), FAILED);
            updateStatus(task.getId(), task.getGroup(), FAILED, MISSING_HANDLER);
            return;
        }
        updateStatus(task.getId(), task.getGroup(), SUBMITTED);
        taskTypeToRunningTasksCount.put(task.getType(), taskTypeToRunningTasksCount.get(task.getType()) + 1);
        taskExecutorService.submit(() -> {
            try {
                updateStatus(task.getId(), task.getGroup(), RUNNING);
                handler.handle(task);
                updateStatus(task.getId(), task.getGroup(), SUCCESSFUL);
            } catch (Exception e) {
                logger.error("Error executing task {}", task, e);
                updateStatus(task.getId(), task.getGroup(), FAILED, e.getMessage());
            } finally {
                synchronized (taskTypeToRunningTasksCount) {
                    taskTypeToRunningTasksCount.put(task.getType(), taskTypeToRunningTasksCount.get(task.getType()) - 1);
                }
            }
        });
    }

    private void updateStatus(String taskId, String taskGroup, Status status) {
        updateStatus(taskId, taskGroup, status, null);
    }

    private void updateStatus(String taskId, String taskGroup, Status status, String statusMessage) {
        try {
            TaskStatus taskStatus = new TaskStatus();
            taskStatus.setTaskId(taskId);
            taskStatus.setTaskGroup(taskGroup);
            taskStatus.setStatus(status);
            taskStatus.setStatusMessage(statusMessage);
            producer.send(statusQueue, MAPPER.writeValueAsString(taskStatus));
        } catch (IOException e) {
            logger.error("Error adding task status {} to queue", status, e);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping task execution service");
        if (consumer != null) {
            consumer.close();
        }
        try {
            internalExecutorService.shutdown();
            internalExecutorService.awaitTermination(10, SECONDS);
            taskExecutorService.shutdown();
            taskExecutorService.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error stopping executor pool", e);
        }
        if (producer != null) {
            producer.close();
        }
    }
}
