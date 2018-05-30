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
import com.cognitree.kronos.executor.handlers.HandlerException;
import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskStatus;
import com.cognitree.kronos.queue.Subscriber;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.cognitree.kronos.model.FailureMessage.MISSING_HANDLER;
import static com.cognitree.kronos.model.Task.Status.*;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * A task execution service is responsible for configuring/ initializing each {@link TaskHandler} and
 * periodically polling new tasks from task queue and submitting it to appropriate handler for execution
 * <p>
 * A task execution service acts as an consumer of tasks from task queue and producer of task status to the
 * task status queue
 * </p>
 */
public class TaskExecutionService implements Service, Subscriber<Task> {
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionService.class);

    // max idle time in minute for an executor thread
    private static final int KEEP_ALIVE_TIME_IN_MINS = 1;

    private final Consumer<Task> taskConsumer;
    private final Producer<TaskStatus> statusProducer;
    private final Map<String, TaskHandlerConfig> taskTypeToHandlerConfig;

    private final Map<String, TaskHandler> taskTypeToHandlerMap = new HashMap<>();
    private final Map<String, ThreadPoolExecutor> taskTypeToExecutorMap = new HashMap<>();

    public TaskExecutionService(Consumer<Task> taskConsumer, Producer<TaskStatus> statusProducer,
                                Map<String, TaskHandlerConfig> handlerConfig) {
        this.taskConsumer = taskConsumer;
        this.statusProducer = statusProducer;
        this.taskTypeToHandlerConfig = handlerConfig;
    }

    @Override
    public void init() {
        logger.info("Initializing task execution service");
        initTaskHandlersAndExecutors();
    }

    private void initTaskHandlersAndExecutors() {
        taskTypeToHandlerConfig.forEach((taskType, taskHandlerConfig) -> {
            try {
                logger.info("Initializing task handler of type {} with config {}", taskType, taskHandlerConfig);
                final TaskHandler taskHandler = (TaskHandler) Class.forName(taskHandlerConfig.getHandlerClass())
                        .newInstance();
                taskHandler.init(taskHandlerConfig.getConfig());
                taskTypeToHandlerMap.put(taskType, taskHandler);

                int maxParallelTasks = taskHandlerConfig.getMaxParallelTasks();
                maxParallelTasks = maxParallelTasks > 0 ? maxParallelTasks : Runtime.getRuntime().availableProcessors();
                ThreadPoolExecutor executor = new ThreadPoolExecutor(maxParallelTasks, maxParallelTasks,
                        KEEP_ALIVE_TIME_IN_MINS, MINUTES, new LinkedBlockingQueue<>());
                taskTypeToExecutorMap.put(taskType, executor);
            } catch (Exception e) {
                logger.error("Error initializing handler of type {} with config {}", taskType, taskHandlerConfig, e);
            }
        });
    }

    @Override
    public void start() {
        taskConsumer.subscribe(this);
    }

    @Override
    public void consume(List<Task> tasks) {
        if (tasks != null && !tasks.isEmpty()) {
            execute(tasks);
        }
    }

    /**
     * submits the list of tasks for execution to appropriate handler based on task type.
     *
     * @param tasks tasks to submit for execution
     */
    private void execute(List<Task> tasks) {
        for (Task task : tasks) {
            logger.trace("Received task {} for execution from task queue", task);
            final TaskHandler handler = taskTypeToHandlerMap.get(task.getType());
            if (handler == null) {
                logger.error("No handler found to execute task {} of type {}, skipping task and marking it as {}",
                        task, task.getType(), FAILED);
                updateStatus(task.getId(), task.getGroup(), FAILED, MISSING_HANDLER);
                continue;
            }
            final ThreadPoolExecutor executor = taskTypeToExecutorMap.get(task.getType());
            executor.submit(() -> {
                try {
                    updateStatus(task.getId(), task.getGroup(), RUNNING);
                    handler.handle(task);
                    updateStatus(task.getId(), task.getGroup(), SUCCESSFUL);
                } catch (Exception e) {
                    logger.error("Error executing task {}", task, e);
                    updateStatus(task.getId(), task.getGroup(), FAILED, e.getMessage());
                }
            });
        }
    }

    private void updateStatus(String taskId, String taskGroup, Task.Status status) {
        updateStatus(taskId, taskGroup, status, null);
    }

    private void updateStatus(String taskId, String taskGroup, Task.Status status, String statusMessage) {
        try {
            TaskStatus taskStatus = new TaskStatus();
            taskStatus.setTaskId(taskId);
            taskStatus.setTaskGroup(taskGroup);
            taskStatus.setStatus(status);
            taskStatus.setStatusMessage(statusMessage);
            statusProducer.add(taskStatus);
        } catch (Exception e) {
            logger.error("Error adding task status {} to queue", status, e);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping task execution service");
        taskTypeToExecutorMap.values().forEach(ThreadPoolExecutor::shutdown);
        try {
            for (ThreadPoolExecutor threadPoolExecutor : taskTypeToExecutorMap.values()) {
                threadPoolExecutor.awaitTermination(1, MINUTES);
            }
        } catch (InterruptedException e) {
            logger.error("Error stopping handler thread pool", e);
        }
        if (taskConsumer != null) {
            taskConsumer.close();
        }
        if (statusProducer != null) {
            statusProducer.close();
        }
    }
}
