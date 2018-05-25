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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ApplicationConfig;
import com.cognitree.kronos.Service;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskDefinition;
import com.cognitree.kronos.model.TaskStatus;
import com.cognitree.kronos.queue.Subscriber;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicy;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.kronos.store.TaskStore;
import com.cognitree.kronos.store.TaskStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.cognitree.kronos.model.FailureMessage.TASK_SUBMISSION_FAILED;
import static com.cognitree.kronos.model.FailureMessage.TIMED_OUT;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.util.DateTimeUtil.resolveDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * A task scheduler service resolves dependency for each submitted task via {@link TaskProvider} and
 * submits the task ready for execution to the task queue.
 * <p>
 * A task scheduler service acts as an producer of task to the task queue and consumer of task status
 * from the task status queue
 * </p>
 */
public final class TaskSchedulerService implements Service, Subscriber<TaskStatus>, TaskStatusHandler {
    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulerService.class);

    private static final String DEFAULT_MAX_EXECUTION_TIME = "1d";

    private final Producer<Task> taskProducer;
    private final Consumer<TaskStatus> statusConsumer;
    private final Map<String, TaskHandlerConfig> taskTypeToHandlerConfig;
    private final Map<String, TimeoutPolicyConfig> policyIdToPolicyConfig;
    private final TaskStoreConfig taskStoreConfig;
    private final String taskPurgeInterval;

    private final Map<String, TimeoutPolicy> timeoutPolicyMap = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> taskTimeoutHandlersMap = new HashMap<>();
    // used by internal tasks for printing the dag/ delete stale tasks/ executing timeout tasks
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private TaskStore taskStore;
    private TaskProvider taskProvider;
    private boolean isInitialized; // is the task scheduler service initialized

    public TaskSchedulerService(Producer<Task> taskProducer, Consumer<TaskStatus> statusConsumer,
                                Map<String, TaskHandlerConfig> handlerConfig, Map<String, TimeoutPolicyConfig> policyConfig,
                                TaskStoreConfig taskStoreConfig, String taskPurgeInterval) {
        logger.info("Initializing task scheduler service with " +
                        "task producer {}, handler config {}, store provider config {}, task purge interval {}",
                taskProducer, handlerConfig, taskStoreConfig, taskPurgeInterval);
        this.taskProducer = taskProducer;
        this.statusConsumer = statusConsumer;
        this.taskTypeToHandlerConfig = handlerConfig;
        this.policyIdToPolicyConfig = policyConfig;
        this.taskStoreConfig = taskStoreConfig;
        this.taskPurgeInterval = taskPurgeInterval;
    }

    /**
     * Task scheduler service is initialized in an order to get back to the last known state
     * Initialization order:
     * <pre>
     * 1) Initialize task provider
     * 2) Subscribe for task status update
     * 3) Initialize configured timeout policies
     * 4) Initialize timeout task for all the active tasks
     * </pre>
     *
     * @throws Exception
     */
    @Override
    public void init() throws Exception {
        initTaskProvider();
        statusConsumer.subscribe(this);
        initTimeoutPolicies();
        initTimeoutTasks();
        isInitialized = true;
    }

    public void reinitTaskProvider() {
        taskProvider.reinit();
    }

    private void initTaskProvider() throws Exception {
        taskStore = (TaskStore) Class.forName(taskStoreConfig.getTaskStoreClass())
                .getConstructor()
                .newInstance();
        taskStore.init(taskStoreConfig.getConfig());
        this.taskProvider = new TaskProvider(taskStore, this);
    }

    private void initTimeoutPolicies() {
        if (policyIdToPolicyConfig != null) {
            policyIdToPolicyConfig.forEach((policyId, policyConfig) -> {
                try {
                    final TimeoutPolicy timeoutPolicy = (TimeoutPolicy) Class.forName(policyConfig.getPolicyClass())
                            .getConstructor()
                            .newInstance();
                    timeoutPolicy.init(policyConfig.getConfig());
                    timeoutPolicyMap.put(policyId, timeoutPolicy);
                } catch (Exception e) {
                    logger.error("Error initializing timeout policy with id {}, config {}", policyId, policyConfig, e);
                }
            });
        }
    }

    /**
     * create timeout tasks for all the active tasks
     */
    private void initTimeoutTasks() {
        taskProvider.getActiveTasks().forEach(this::createTimeoutTask);
    }

    private void createTimeoutTask(Task task) {
        if (taskTimeoutHandlersMap.containsKey(task.getId())) {
            logger.debug("Timeout task is already scheduled for task {}", task.getId());
            return;
        }

        final long timeoutTaskTime = task.getSubmittedAt() + getMaxExecutionTime(task);
        final long currentTimeMillis = System.currentTimeMillis();

        final TimeoutPolicy timeoutPolicy = resolveTimeoutPolicy(task);
        final TimeoutTask timeoutTask = new TimeoutTask(task, timeoutPolicy);
        if (timeoutTaskTime < currentTimeMillis) {
            // submit timeout task now
            scheduledExecutorService.submit(timeoutTask);
        } else {
            logger.info("Initializing timeout task for task {}, scheduled at {}", task.getId(), timeoutTaskTime);
            final ScheduledFuture<?> timeoutTaskFuture =
                    scheduledExecutorService.schedule(timeoutTask, timeoutTaskTime - currentTimeMillis, MILLISECONDS);
            taskTimeoutHandlersMap.put(task.getId(), timeoutTaskFuture);
        }
    }

    /**
     * resolve timeout policy for task, policy resolution is done at multiple levels.
     * First policy is checked at task definition level defined as {@link TaskDefinition#timeoutPolicy}
     * If not found then it is checked at the task handler level defined as {@link TaskHandlerConfig#timeoutPolicy}
     *
     * @param task
     * @return
     */
    private TimeoutPolicy resolveTimeoutPolicy(Task task) {
        final String timeoutPolicyId = task.getTimeoutPolicy();
        if (timeoutPolicyId != null && timeoutPolicyMap.containsKey(timeoutPolicyId)) {
            return timeoutPolicyMap.get(timeoutPolicyId);
        }

        final TaskHandlerConfig taskHandlerConfig = taskTypeToHandlerConfig.get(task.getType());
        if (taskHandlerConfig == null) {
            return null;
        }
        return timeoutPolicyMap.get(taskHandlerConfig.getTimeoutPolicy());
    }

    @Override
    public void start() {
        scheduledExecutorService.scheduleAtFixedRate(() -> logger.debug("{}", taskProvider.toString()), 5, 5, MINUTES);
        scheduledExecutorService.scheduleAtFixedRate(this::deleteStaleTasks, 5, 5, MINUTES);
    }

    public void add(Task task) {
        logger.info("Received request to add task: {}", task);
        taskProvider.addTask(task);
    }

    @Override
    public void consume(List<TaskStatus> tasksStatus) {
        for (TaskStatus taskStatus : tasksStatus) {
            taskProvider.updateTask(taskStatus.getTaskId(), taskStatus.getTaskGroup(),
                    taskStatus.getStatus(), taskStatus.getStatusMessage());
        }
    }

    @Override
    public void onStatusChange(Task task) {
        switch (task.getStatus()) {
            case WAITING:
                scheduleReadyTasks();
                break;
            case SUBMITTED:
                createTimeoutTask(task);
                break;
            case SUCCESSFUL:
            case FAILED:
                final ScheduledFuture<?> taskTimeoutFuture = taskTimeoutHandlersMap.remove(task.getId());
                if (taskTimeoutFuture != null) {
                    taskTimeoutFuture.cancel(false);
                }
                // If the task is finished (reached terminal state), proceed to schedule the next set of tasks
                scheduleReadyTasks();
                break;
        }
    }

    /**
     * submit tasks in created state with resolved dependency
     */
    private void scheduleReadyTasks() {
        if (isInitialized) {
            final List<Task> runnableTaskList = taskProvider.getReadyTasks();
            for (Task task : runnableTaskList) {
                try {
                    taskProducer.add(task);
                    taskProvider.updateTask(task, SUBMITTED, null);
                } catch (Exception e) {
                    logger.error("Error submitting task {} to queue", task, e);
                    taskProvider.updateTask(task, FAILED, TASK_SUBMISSION_FAILED);
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping task scheduler service");
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(1, MINUTES);
        } catch (InterruptedException e) {
            logger.error("Error stopping scheduled thread pool", e);
        }
        if (taskProducer != null) {
            taskProducer.close();
        }
        if (statusConsumer != null) {
            statusConsumer.close();
        }
        if (taskStore != null) {
            taskStore.stop();
        }
    }

    private long getMaxExecutionTime(Task task) {
        if (task.getMaxExecutionTime() != null) {
            return resolveDuration(task.getMaxExecutionTime());
        }

        final TaskHandlerConfig taskHandlerConfig = taskTypeToHandlerConfig.get(task.getType());
        if (taskHandlerConfig != null && taskHandlerConfig.getMaxExecutionTime() != null) {
            return resolveDuration(taskHandlerConfig.getMaxExecutionTime());
        }

        return resolveDuration(DEFAULT_MAX_EXECUTION_TIME);
    }

    /**
     * deletes all the stale tasks from memory
     * task to delete is determined by {@link ApplicationConfig#taskPurgeInterval}
     * <p>
     * see: {@link ApplicationConfig#taskPurgeInterval} for more details and implication of taskPurgeInterval
     */
    void deleteStaleTasks() {
        taskProvider.removeOldTasks(taskPurgeInterval);
    }

    // used in junit
    TaskProvider getTaskProvider() {
        return taskProvider;
    }

    private class TimeoutTask implements Runnable {

        private final Task task;
        private final TimeoutPolicy timeoutPolicy;

        TimeoutTask(Task task, TimeoutPolicy timeoutPolicy) {
            this.task = task;
            this.timeoutPolicy = timeoutPolicy;
        }

        @Override
        public void run() {
            logger.info("Task {} has timed out, marking task as failed", task);
            taskProvider.updateTask(task, FAILED, TIMED_OUT);
            if (timeoutPolicy != null) {
                logger.info("Applying timeout policy {} on task {}", timeoutPolicy, task);
                timeoutPolicy.handle(task);
            }
        }
    }
}
