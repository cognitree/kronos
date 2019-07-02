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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.cognitree.kronos.model.Messages.MISSING_TASK_HANDLER_MESSAGE;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.queue.QueueService.EXECUTOR_QUEUE;
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
    private final Map<String, TaskHandlerConfig> taskTypeToHandlerConfigMap;

    private final Map<String, Integer> taskTypeToMaxParallelTasksCount = new HashMap<>();
    private final Map<String, Integer> taskTypeToRunningTasksCount = new HashMap<>();
    private final Map<TaskExecutionId, TaskHandler> taskHandlersMap = new ConcurrentHashMap<>();
    private final Map<TaskExecutionId, Future<TaskResult>> taskFuturesMap = new ConcurrentHashMap<>();

    // used by internal tasks to poll new tasks from queue
    private final ScheduledExecutorService taskConsumerThreadPool = Executors.newSingleThreadScheduledExecutor();
    // used by internal tasks to poll new control messages from queue
    private final ScheduledExecutorService controlMessageConsumerThreadPool = Executors.newSingleThreadScheduledExecutor();
    // used by internal tasks to check task execution status
    private final ScheduledExecutorService taskCompletionThreadPool = Executors.newSingleThreadScheduledExecutor();

    // used to execute tasks
    private final ExecutorService taskExecutorThreadPool = Executors.newCachedThreadPool();
    private long pollIntervalInMs;

    public TaskExecutionService(Map<String, TaskHandlerConfig> taskTypeToHandlerConfigMap, long pollIntervalInMs) {
        if (taskTypeToHandlerConfigMap == null || taskTypeToHandlerConfigMap.isEmpty()) {
            logger.error("missing one or more mandatory configuration: taskHandlerConfig");
            throw new IllegalArgumentException("missing one or more mandatory configuration: taskHandlerConfig");
        }
        this.pollIntervalInMs = pollIntervalInMs;
        this.taskTypeToHandlerConfigMap = taskTypeToHandlerConfigMap;
    }

    public static TaskExecutionService getService() {
        return (TaskExecutionService) ServiceProvider.getService(TaskExecutionService.class.getSimpleName());
    }

    @Override
    public void init() {
        logger.info("Initializing task execution service");
        initCounters();
    }

    private void initCounters() {
        for (Map.Entry<String, TaskHandlerConfig> taskTypeToHandlerConfigEntry : taskTypeToHandlerConfigMap.entrySet()) {
            final String taskType = taskTypeToHandlerConfigEntry.getKey();
            final TaskHandlerConfig taskHandlerConfig = taskTypeToHandlerConfigEntry.getValue();
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
        controlMessageConsumerThreadPool.scheduleAtFixedRate(this::consumeControlMessages, 0, pollIntervalInMs, MILLISECONDS);
        taskCompletionThreadPool.scheduleAtFixedRate(new TaskCompletionChecker(), 0, pollIntervalInMs, MILLISECONDS);
        ServiceProvider.registerService(this);
    }

    private void consumeTasks() {
        taskTypeToMaxParallelTasksCount.forEach((taskType, maxParallelTasks) -> {
            synchronized (taskType) {
                final int maxTasksToPoll = maxParallelTasks - taskTypeToRunningTasksCount.get(taskType);
                if (maxTasksToPoll > 0) {
                    try {
                        final List<Task> tasks = QueueService.getService(EXECUTOR_QUEUE).consumeTask(taskType, maxTasksToPoll);
                        tasks.forEach(this::submit);
                    } catch (ServiceException e) {
                        logger.error("Error consuming tasks for execution", e);
                    }
                }
            }
        });
    }

    private void consumeControlMessages() {
        final List<ControlMessage> controlMessages;
        try {
            controlMessages = QueueService.getService(EXECUTOR_QUEUE).consumeControlMessages();
        } catch (ServiceException e) {
            logger.error("Error consuming control messages", e);
            return;
        }

        for (ControlMessage controlMessage : controlMessages) {
            logger.info("Received request to execute control message {}", controlMessage);
            final Task task = controlMessage.getTask();
            final TaskExecutionId taskExecutionId = new TaskExecutionId(task, task.getRetryCount());
            if (!taskFuturesMap.containsKey(taskExecutionId)) {
                continue;
            }
            final Future<TaskResult> taskResultFuture = taskFuturesMap.get(taskExecutionId);
            if (taskResultFuture != null) {
                switch (controlMessage.getAction()) {
                    case ABORT:
                    case TIME_OUT:
                        logger.info("Received request to {} task with id {}",
                                controlMessage.getAction(), task.getIdentity());
                        // interrupt the task first and then call the abort method
                        taskResultFuture.cancel(true);
                        taskHandlersMap.get(taskExecutionId).abort();
                }
            }
        }
    }

    /**
     * submit the task for execution to appropriate handler based on task type.
     *
     * @param task task to submit for execution
     */
    private void submit(Task task) {
        logger.info("Received request to submit task for execution: {}", task.getIdentity());
        final TaskHandler taskHandler;
        try {
            final TaskHandlerConfig taskHandlerConfig = taskTypeToHandlerConfigMap.get(task.getType());
            taskHandler = (TaskHandler) Class.forName(taskHandlerConfig.getHandlerClass())
                    .getConstructor()
                    .newInstance();
            taskHandler.init(task, taskHandlerConfig.getConfig());
        } catch (InstantiationException | InvocationTargetException | NoSuchMethodException
                | IllegalAccessException | ClassNotFoundException e) {
            logger.error("Error initializing handler for task {}", task, e);
            sendTaskStatusUpdate(task, FAILED, MISSING_TASK_HANDLER_MESSAGE);
            return;
        }
        taskTypeToRunningTasksCount.put(task.getType(), taskTypeToRunningTasksCount.get(task.getType()) + 1);
        final Future<TaskResult> taskResultFuture = taskExecutorThreadPool.submit(() -> {
            logger.debug("Executing task {}", task.getIdentity());
            sendTaskStatusUpdate(task, RUNNING, null);
            return taskHandler.execute();
        });
        final TaskExecutionId taskExecutionId = new TaskExecutionId(task, task.getRetryCount());
        taskHandlersMap.put(taskExecutionId, taskHandler);
        taskFuturesMap.put(taskExecutionId, taskResultFuture);
    }

    private void sendTaskStatusUpdate(TaskId taskId, Status status, String statusMessage) {
        sendTaskStatusUpdate(taskId, status, statusMessage, null);
    }

    private void sendTaskStatusUpdate(TaskId taskId, Status status, String statusMessage, Map<String, Object> context) {
        try {
            final TaskStatusUpdate taskStatusUpdate = new TaskStatusUpdate();
            taskStatusUpdate.setTaskId(taskId);
            taskStatusUpdate.setStatus(status);
            taskStatusUpdate.setStatusMessage(statusMessage);
            taskStatusUpdate.setContext(context);
            QueueService.getService(EXECUTOR_QUEUE).send(taskStatusUpdate);
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
            controlMessageConsumerThreadPool.shutdown();
            controlMessageConsumerThreadPool.awaitTermination(10, SECONDS);
            taskExecutorThreadPool.shutdown();
            taskExecutorThreadPool.awaitTermination(10, SECONDS);
            taskCompletionThreadPool.shutdown();
            taskCompletionThreadPool.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error stopping executor pool", e);
        }
    }

    private class TaskCompletionChecker implements Runnable {
        @Override
        public void run() {
            final ArrayList<TaskExecutionId> completedTasks = new ArrayList<>();
            taskFuturesMap.forEach((taskHandler, future) -> {
                Task task = taskHandler.getTask();
                logger.debug("Checking task {} for completion", task.getIdentity());
                if (future.isDone()) {
                    try {
                        TaskResult taskResult = future.get();
                        if (taskResult.isSuccess()) {
                            sendTaskStatusUpdate(task, SUCCESSFUL, taskResult.getMessage(), taskResult.getContext());
                        } else {
                            sendTaskStatusUpdate(task, FAILED, taskResult.getMessage(), taskResult.getContext());
                        }
                    } catch (InterruptedException e) {
                        logger.error("Thread interrupted waiting for task result for task {}", task.getIdentity(), e);
                    } catch (CancellationException e) {
                        logger.info("Task {} has been aborted", task.getIdentity());
                        // do nothing the task is already marked as aborted
                    } catch (ExecutionException e) {
                        logger.error("Error executing task {}", task.getIdentity(), e);
                        sendTaskStatusUpdate(task, FAILED, "error executing task: " + e.getMessage());
                    } finally {
                        synchronized (task.getType()) {
                            taskTypeToRunningTasksCount.put(task.getType(), taskTypeToRunningTasksCount.get(task.getType()) - 1);
                        }
                        completedTasks.add(taskHandler);
                    }
                }
            });
            if (!completedTasks.isEmpty()) {
                logger.debug("Tasks {} completed execution", completedTasks.stream().map(t -> t.getTask().getIdentity())
                        .collect(Collectors.toList()));
            }
            completedTasks.forEach(taskFuturesMap::remove);
            completedTasks.forEach(taskHandlersMap::remove);
        }
    }

    private class TaskExecutionId {
        private final Task task;
        private final int executionId;

        private TaskExecutionId(Task task, int executionId) {
            this.task = task;
            this.executionId = executionId;
        }

        public Task getTask() {
            return task;
        }

        public int getExecutionId() {
            return executionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TaskExecutionId)) return false;
            TaskExecutionId taskExecutionId = (TaskExecutionId) o;
            return executionId == taskExecutionId.executionId &&
                    Objects.equals(task, taskExecutionId.task);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task, executionId);
        }
    }
}
