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
import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;
import com.cognitree.kronos.executor.model.TaskResult;
import com.cognitree.kronos.model.ControlMessage;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.model.TaskStatusUpdate;
import com.cognitree.kronos.queue.QueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A task execution service is responsible for initializing each {@link TaskHandler} and periodically polling new tasks
 * from queue and submitting it to appropriate handler for execution.
 * <p>
 * A task execution service acts as an consumer of tasks from queue and producer of task result to the queue.
 * </p>
 */
public final class TaskExecutionService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionService.class);

    // Task type mapping Info
    private final Map<String, TaskHandlerConfig> taskTypeToHandlerConfig;
    private final Map<String, TaskHandler> taskTypeToHandlerMap = new HashMap<>();
    private final Map<String, Integer> taskTypeToMaxParallelTasksCount = new HashMap<>();
    private final Map<String, Integer> taskTypeToRunningTasksCount = new HashMap<>();
    // used by internal tasks like polling new tasks from queue
    private final ScheduledExecutorService taskConsumerThreadPool = Executors.newSingleThreadScheduledExecutor();
    // used to execute tasks
    private final ExecutorService taskExecutorThreadPool = Executors.newCachedThreadPool();
    private long pollIntervalInMs;

    public TaskExecutionService(Map<String, TaskHandlerConfig> taskTypeToHandlerConfig, long pollIntervalInMs) {
        if (taskTypeToHandlerConfig == null || taskTypeToHandlerConfig.isEmpty()) {
            logger.error("missing one or more mandatory configuration: " +
                    "taskHandlerConfig");
            throw new IllegalArgumentException("missing one or more mandatory configuration: " +
                    "taskHandlerConfig");
        }
        this.pollIntervalInMs = pollIntervalInMs;
        this.taskTypeToHandlerConfig = taskTypeToHandlerConfig;
    }

    public static TaskExecutionService getService() {
        return (TaskExecutionService) ServiceProvider.getService(TaskExecutionService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        logger.info("Initializing task execution service");
        initTaskHandlersAndExecutors();
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
        logger.info("Starting task execution service");
        taskConsumerThreadPool.scheduleAtFixedRate(this::consumeTasks, 0, pollIntervalInMs, MILLISECONDS);
        taskConsumerThreadPool.scheduleAtFixedRate(this::consumeTasks, 0, pollIntervalInMs, MILLISECONDS);
        ServiceProvider.registerService(this);
    }

    private void consumeTasks() {
        taskTypeToMaxParallelTasksCount.forEach((taskType, maxParallelTasks) -> {
            synchronized (taskTypeToRunningTasksCount) {
                final int tasksToPoll = maxParallelTasks - taskTypeToRunningTasksCount.get(taskType);
                if (tasksToPoll > 0) {
                    try {
                        final List<Task> tasks = QueueService.getService().consumeTask(taskType, tasksToPoll);
                        for (Task task : tasks) {
                            submit(task);
                        }
                    } catch (ServiceException e) {
                        logger.error("Error consuming tasks for execution", e);
                    }
                }
            }
        });
    }

    private void consumeControlMessages() {
        try {
            final List<ControlMessage> controlMessages = QueueService.getService().consumeControlMessage();
        } catch (ServiceException e) {
            logger.info("Error consuming control messages", e);
        }
    }

    /**
     * submit the task for execution to appropriate handler based on task type.
     *
     * @param task task to submit for execution
     */
    private void submit(Task task) {
        logger.trace("Received task {} for execution from task queue", task);
        sendTaskUpdate(task, SUBMITTED);
        taskTypeToRunningTasksCount.put(task.getType(), taskTypeToRunningTasksCount.get(task.getType()) + 1);
        taskExecutorThreadPool.submit(() -> {
            try {
                sendTaskUpdate(task, RUNNING);
                final TaskHandler handler = taskTypeToHandlerMap.get(task.getType());
                final TaskResult taskResult = handler.handle(task);
                if (taskResult.isSuccess()) {
                    sendTaskUpdate(task, SUCCESSFUL, taskResult.getMessage(), taskResult.getContext());
                } else {
                    sendTaskUpdate(task, FAILED, taskResult.getMessage(), taskResult.getContext());
                }
            } catch (Exception e) {
                logger.error("Error executing task {}", task, e);
                sendTaskUpdate(task, FAILED, e.getMessage());
            } finally {
                synchronized (taskTypeToRunningTasksCount) {
                    taskTypeToRunningTasksCount.put(task.getType(), taskTypeToRunningTasksCount.get(task.getType()) - 1);
                }
            }
        });
    }

    private void sendTaskUpdate(TaskId taskId, Status status) {
        sendTaskUpdate(taskId, status, null);
    }

    private void sendTaskUpdate(TaskId taskId, Status status, String statusMessage) {
        sendTaskUpdate(taskId, status, statusMessage, null);
    }

    private void sendTaskUpdate(TaskId taskId, Status status, String statusMessage, Map<String, Object> context) {
        try {
            final TaskStatusUpdate taskStatusUpdate = new TaskStatusUpdate();
            taskStatusUpdate.setTaskId(taskId);
            taskStatusUpdate.setStatus(status);
            taskStatusUpdate.setStatusMessage(statusMessage);
            taskStatusUpdate.setContext(context);
            QueueService.getService().send(taskStatusUpdate);
        } catch (ServiceException e) {
            logger.error("Error adding task status {} to queue", status, e);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping task execution service");
        try {
            taskConsumerThreadPool.shutdown();
            taskConsumerThreadPool.awaitTermination(10, SECONDS);
            taskExecutorThreadPool.shutdown();
            taskExecutorThreadPool.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error stopping executor pool", e);
        }
    }
}
