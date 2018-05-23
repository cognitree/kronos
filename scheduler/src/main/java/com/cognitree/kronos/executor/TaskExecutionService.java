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
import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskStatus;
import com.cognitree.kronos.queue.Subscriber;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.producer.Producer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.cognitree.kronos.model.FailureMessage.HANDLER_FAILURE;
import static com.cognitree.kronos.model.FailureMessage.MISSING_HANDLER;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * A task execution service is responsible for configuring/ initializing each {@link TaskHandler} and
 * periodically polling new tasks from task queue and submitting it to appropriate handler for execution
 * <p>
 * A task execution service acts as an consumer of tasks from task queue and producer of task status to the
 * task status queue
 * </p>
 */
public class TaskExecutionService implements Service, TaskStatusListener, Subscriber<Task> {
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
                taskHandler.init(taskHandlerConfig.getConfig(), this);
                taskTypeToHandlerMap.put(taskType, taskHandler);

                int maxParallelTasks = taskHandlerConfig.getMaxParallelTasks();
                maxParallelTasks = maxParallelTasks > 0 ? maxParallelTasks : Runtime.getRuntime().availableProcessors();
                ThreadPoolExecutor executor = new ThreadPoolExecutor((int) Math.ceil(maxParallelTasks / 2), maxParallelTasks,
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
     * executes the submitted list of tasks by calling appropriate {@link TaskHandler} for each task
     * Also creates a timeout tasks which will be executed if a task status (one of SUCCESSFUL/ FAILED)
     * is not received before max task execution time
     *
     * @param tasks
     */
    private void execute(List<Task> tasks) {
        for (Task task : tasks) {
            logger.trace("Received task {} for execution from task queue", task);
            final TaskHandler handler = getTaskHandler(task.getType());
            if (handler == null) {
                logger.error("No handler found to execute task {} of type {}, skipping task {} and marking it as {}",
                        task, task.getType(), FAILED);
                updateStatus(task.getId(), task.getGroup(), FAILED, MISSING_HANDLER);
                continue;
            }
            final ThreadPoolExecutor executor = getTaskExecutor(task);
            executor.submit(() -> {
                try {
                    updateStatus(task.getId(), task.getGroup(), RUNNING);
                    handler.handle(task);
                } catch (Exception e) {
                    logger.error("Error executing task {}", task, e);
                    updateStatus(task.getId(), task.getGroup(), FAILED, HANDLER_FAILURE);
                }
            });
        }
    }

    @Override
    public void updateStatus(String taskId, String taskGroup, Task.Status status) {
        updateStatus(taskId, taskGroup, status, null);
    }

    @Override
    public void updateStatus(String taskId, String taskGroup, Task.Status status, String statusMessage) {
        updateStatus(taskId, taskGroup, status, null, null);
    }

    @Override
    public void updateStatus(String taskId, String taskGroup, Task.Status status, String statusMessage, ObjectNode properties) {
        try {
            TaskStatus taskStatus = new TaskStatus();
            taskStatus.setTaskId(taskId);
            taskStatus.setTaskGroup(taskGroup);
            taskStatus.setStatus(status);
            taskStatus.setStatusMessage(statusMessage);
            taskStatus.setRuntimeProperties(properties);
            statusProducer.add(taskStatus);
        } catch (Exception e) {
            logger.error("Error adding task status {} to queue", status, e);
        }
    }

    private TaskHandler getTaskHandler(String taskType) {
        return taskTypeToHandlerMap.getOrDefault(taskType, taskTypeToHandlerMap.get("default"));
    }

    private ThreadPoolExecutor getTaskExecutor(Task task) {
        return taskTypeToExecutorMap.getOrDefault(task.getType(), taskTypeToExecutorMap.get("default"));
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
