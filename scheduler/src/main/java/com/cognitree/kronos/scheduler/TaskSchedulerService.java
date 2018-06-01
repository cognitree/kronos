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
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskDefinition;
import com.cognitree.kronos.model.TaskStatus;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.queue.producer.ProducerConfig;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicy;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.kronos.store.TaskStore;
import com.cognitree.kronos.store.TaskStoreConfig;
import com.cognitree.kronos.util.DateTimeUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.cognitree.kronos.model.FailureMessage.*;
import static com.cognitree.kronos.model.Task.Status.*;
import static com.cognitree.kronos.util.DateTimeUtil.resolveDuration;
import static java.util.concurrent.TimeUnit.*;

/**
 * A task scheduler service resolves dependency for each submitted task via {@link TaskProvider} and
 * submits the task ready for execution to the task queue.
 * <p>
 * A task scheduler service acts as an producer of task to the task queue and consumer of task status
 * from the task status queue
 * </p>
 */
public final class TaskSchedulerService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulerService.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DEFAULT_MAX_EXECUTION_TIME = "1d";
    private static final String TASK_STATUS_TOPIC = "taskstatus";

    private final ProducerConfig producerConfig;
    private final ConsumerConfig consumerConfig;
    private final Map<String, TaskHandlerConfig> taskTypeToHandlerConfig;
    private final Map<String, TimeoutPolicyConfig> policyIdToPolicyConfig;
    private final TaskStoreConfig taskStoreConfig;
    private final String taskPurgeInterval;

    private final Map<String, TimeoutPolicy> timeoutPolicyMap = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> taskTimeoutHandlersMap = new HashMap<>();
    // used by internal tasks for printing the dag/ delete stale tasks/ executing timeout tasks
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private final Set<TaskStatusChangeListener> statusChangeListeners = new HashSet<>();

    private Producer producer;
    private Consumer consumer;
    private TaskStore taskStore;
    private TaskProvider taskProvider;
    private boolean isInitialized; // is the task scheduler service initialized

    public TaskSchedulerService(ProducerConfig producerConfig, ConsumerConfig consumerConfig,
                                Map<String, TaskHandlerConfig> handlerConfig, Map<String, TimeoutPolicyConfig> policyConfig,
                                TaskStoreConfig taskStoreConfig, String taskPurgeInterval) {
        this.producerConfig = producerConfig;
        this.consumerConfig = consumerConfig;
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
        initProducer();
        initConsumer();
        initTimeoutPolicies();
        initTimeoutTasks();
        resolveCreatedTasks();
        isInitialized = true;
    }

    private void initTaskProvider() throws Exception {
        logger.info("Initializing task store with config {}", taskStoreConfig);
        taskStore = (TaskStore) Class.forName(taskStoreConfig.getTaskStoreClass())
                .getConstructor()
                .newInstance();
        taskStore.init(taskStoreConfig.getConfig());
        taskProvider = new TaskProvider(taskStore);
    }

    // used in junit to reinitialize task provider from store
    void reinitTaskProvider() {
        taskProvider.reinit();
        resolveCreatedTasks();
        scheduleReadyTasks();
    }

    private void initConsumer() throws Exception {
        logger.info("Initializing consumer with config {}", consumerConfig);
        consumer = (Consumer) Class.forName(consumerConfig.getConsumerClass())
                .getConstructor()
                .newInstance();
        consumer.init(consumerConfig.getConfig());
        consumeTaskStatus();
    }

    private void initProducer() throws Exception {
        logger.info("Initializing producer with config {}", producerConfig);
        producer = (Producer) Class.forName(producerConfig.getProducerClass())
                .getConstructor()
                .newInstance();
        producer.init(producerConfig.getConfig());
    }

    private void initTimeoutPolicies() {
        if (policyIdToPolicyConfig != null) {
            policyIdToPolicyConfig.forEach((policyId, policyConfig) -> {
                try {
                    logger.info("Initializing timeout policy with id {}, config {}", policyId, policyConfig);
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

    private void resolveCreatedTasks() {
        final List<Task> tasks = taskProvider.getTasks(Collections.singletonList(CREATED));
        tasks.sort(Comparator.comparing(Task::getCreatedAt));
        tasks.forEach(this::resolve);
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
        final long pollInterval = DateTimeUtil.resolveDuration(consumerConfig.getPollInterval());
        scheduledExecutorService.scheduleAtFixedRate(this::consumeTaskStatus, pollInterval, pollInterval, MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(() -> logger.debug("{}", taskProvider.toString()), 5, 5, MINUTES);
        scheduledExecutorService.scheduleAtFixedRate(this::deleteStaleTasks, 5, 5, MINUTES);
    }

    public void registerListener(TaskStatusChangeListener statusChangeListener) {
        statusChangeListeners.add(statusChangeListener);
    }

    public void deregisterListener(TaskStatusChangeListener statusChangeListener) {
        statusChangeListeners.remove(statusChangeListener);
    }

    public void schedule(Task task) {
        logger.info("Received request to schedule task: {}", task);
        add(task);
        resolve(task);
    }

    private void add(Task task) {
        taskProvider.add(task);
    }

    private void resolve(Task task) {
        final boolean isResolved = taskProvider.resolve(task);
        if (isResolved) {
            updateStatus(task, WAITING, null);
        } else {
            logger.error("Unable to resolve dependency for task {}, marking it as {}", task, FAILED);
            updateStatus(task, FAILED, FAILED_TO_RESOLVE_DEPENDENCY);
        }
    }

    private void consumeTaskStatus() {
        final List<String> tasksStatus = consumer.poll(TASK_STATUS_TOPIC);
        for (String taskStatusAsString : tasksStatus) {
            try {
                final TaskStatus taskStatus = MAPPER.readValue(taskStatusAsString, TaskStatus.class);
                updateStatus(taskStatus.getTaskId(), taskStatus.getTaskGroup(),
                        taskStatus.getStatus(), taskStatus.getStatusMessage());
            } catch (IOException e) {
                logger.error("Error parsing task status message {}", taskStatusAsString, e);
            }
        }
    }

    private void updateStatus(String taskId, String taskGroup, Status status,
                              String statusMessage) {
        final Task task = taskProvider.getTask(taskId, taskGroup);
        if (task == null) {
            logger.error("No task found with id {}, group {}", taskId, taskGroup);
            return;
        }
        updateStatus(task, status, statusMessage);
    }

    private void updateStatus(Task task, Status status, String statusMessage) {
        logger.info("Received request to update status of task {} to {} " +
                "with status message {}", task, status, statusMessage);
        Status currentStatus = task.getStatus();
        if (!isValidTransition(currentStatus, status)) {
            logger.error("Invalid state transition for task {} from status {}, to {}", task, task.getStatus(), status);
            return;
        }

        task.setStatus(status);
        task.setStatusMessage(statusMessage);
        switch (status) {
            case CREATED:
                break;
            case WAITING:
                scheduleReadyTasks();
                break;
            case SCHEDULED:
                break;
            case SUBMITTED:
                task.setSubmittedAt(System.currentTimeMillis());
                createTimeoutTask(task);
                break;
            case FAILED:
                markDependentTasksAsFailed(task);
                // do not break
            case SUCCESSFUL:
                task.setCompletedAt(System.currentTimeMillis());
                final ScheduledFuture<?> taskTimeoutFuture = taskTimeoutHandlersMap.remove(task.getId());
                if (taskTimeoutFuture != null) {
                    taskTimeoutFuture.cancel(false);
                }
                // If the task is finished (reached terminal state), proceed to schedule the next set of tasks
                scheduleReadyTasks();
                break;
        }
        taskProvider.update(task);
        statusChangeListeners.forEach(listener -> listener.statusChanged(task, currentStatus, status));
    }

    private boolean isValidTransition(Status currentStatus, Status desiredStatus) {
        switch (desiredStatus) {
            case CREATED:
                return currentStatus == null;
            case WAITING:
                return currentStatus == CREATED;
            case SCHEDULED:
                return currentStatus == WAITING;
            case SUBMITTED:
                return currentStatus == SCHEDULED;
            case RUNNING:
                return currentStatus == SUBMITTED;
            case SUCCESSFUL:
            case FAILED:
                return currentStatus != SUCCESSFUL && currentStatus != FAILED;
            default:
                return false;
        }
    }

    private void markDependentTasksAsFailed(Task task) {
        taskProvider.getDependentTasks(task)
                .forEach(dependentTask -> updateStatus(dependentTask, FAILED, FAILED_TO_RESOLVE_DEPENDENCY));
    }

    /**
     * submit tasks in created state with resolved dependency
     */
    private void scheduleReadyTasks() {
        if (isInitialized) {
            final List<Task> runnableTaskList = taskProvider.getReadyTasks();
            for (Task task : runnableTaskList) {
                try {
                    producer.send(task.getType(), MAPPER.writeValueAsString(task));
                    updateStatus(task, SCHEDULED, null);
                } catch (Exception e) {
                    logger.error("Error submitting task {} to queue", task, e);
                    updateStatus(task, FAILED, TASK_SUBMISSION_FAILED);
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping task scheduler service");
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error stopping scheduled thread pool", e);
        }
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
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
            updateStatus(task, FAILED, TIMED_OUT);
            if (timeoutPolicy != null) {
                logger.info("Applying timeout policy {} on task {}", timeoutPolicy, task);
                timeoutPolicy.handle(task);
            }
        }
    }
}
