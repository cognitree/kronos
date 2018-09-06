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

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.MutableTaskId;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.model.TaskUpdate;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.queue.producer.ProducerConfig;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicy;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.kronos.util.DateTimeUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.cognitree.kronos.model.Task.Status.CREATED;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.model.Task.Status.WAITING;
import static com.cognitree.kronos.scheduler.model.Messages.FAILED_TO_RESOLVE_DEPENDENCY;
import static com.cognitree.kronos.scheduler.model.Messages.TASK_SUBMISSION_FAILED;
import static com.cognitree.kronos.scheduler.model.Messages.TIMED_OUT;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A task scheduler service resolves dependency via {@link TaskProvider} for each submitted task and
 * submits the task ready for execution to the queue via {@link Producer}.
 * <p>
 * A task scheduler service acts as an producer of task to the queue and consumer of task status
 * from the queue
 * </p>
 */
public final class TaskSchedulerService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulerService.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    // Periodically, tasks older than the specified interval and status of workflow (job)
    // it belongs to in one of the final state are purged from memory to prevent the system from going OOM.
    // task purge interval in hour
    private static final int TASK_PURGE_INTERVAL = 1;
    private static final List<Status> NON_FINAL_TASK_STATUS_LIST = new ArrayList<>();

    static {
        for (Status status : Status.values()) {
            if (!status.isFinal()) {
                NON_FINAL_TASK_STATUS_LIST.add(status);
            }
        }
    }

    private final ProducerConfig producerConfig;
    private final ConsumerConfig consumerConfig;
    private final Map<String, TimeoutPolicyConfig> policyConfigMap;
    private final String statusQueue;
    private final Map<String, TimeoutPolicy> timeoutPolicyMap = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> taskTimeoutHandlersMap = new HashMap<>();
    // used by internal tasks for printing the dag/ delete stale tasks/ executing timeout tasks
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private final Set<TaskStatusChangeListener> statusChangeListeners = new HashSet<>();
    private Producer producer;
    private Consumer consumer;
    private TaskProvider taskProvider;
    private boolean isInitialized; // is the task scheduler service initialized

    public TaskSchedulerService(SchedulerConfig schedulerConfig, QueueConfig queueConfig) {
        this.policyConfigMap = schedulerConfig.getTimeoutPolicyConfig();
        this.producerConfig = queueConfig.getProducerConfig();
        this.consumerConfig = queueConfig.getConsumerConfig();
        this.statusQueue = queueConfig.getTaskStatusQueue();
    }

    public static TaskSchedulerService getService() {
        return (TaskSchedulerService) ServiceProvider.getService(TaskSchedulerService.class.getSimpleName());
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
        logger.info("Initializing task scheduler service");
        initTaskProvider();
        initProducer();
        initConsumer();
        initTimeoutPolicies();
        initTimeoutTasks();
        resolveCreatedTasks();
        isInitialized = true;
    }

    private void initTaskProvider() throws ServiceException {
        logger.info("Initializing task provider");
        taskProvider = new TaskProvider();
        logger.info("Initializing task provider from task store");
        final List<Namespace> namespaces = NamespaceService.getService().get();
        final List<Task> tasks = new ArrayList<>();
        for (Namespace namespace : namespaces) {
            TaskService.getService().get(NON_FINAL_TASK_STATUS_LIST, namespace.getName());
        }
        if (!tasks.isEmpty()) {
            tasks.sort(Comparator.comparing(Task::getCreatedAt));
            tasks.forEach(taskProvider::add);
            tasks.forEach(this::resolve);
        }
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

    private void initTimeoutPolicies() throws Exception {
        for (Map.Entry<String, TimeoutPolicyConfig> policyEntry : policyConfigMap.entrySet()) {
            final String policyId = policyEntry.getKey();
            final TimeoutPolicyConfig policyConfig = policyEntry.getValue();
            logger.info("Initializing timeout policy with id {}, config {}", policyId, policyConfig);
            final TimeoutPolicy timeoutPolicy = (TimeoutPolicy) Class.forName(policyConfig.getPolicyClass())
                    .getConstructor()
                    .newInstance();
            timeoutPolicy.init(policyConfig.getConfig());
            timeoutPolicyMap.put(policyId, timeoutPolicy);
        }
    }

    /**
     * create timeout tasks for all the active tasks
     */
    private void initTimeoutTasks() {
        taskProvider.getActiveTasks().forEach(this::createTimeoutTask);
    }

    private void createTimeoutTask(Task task) {
        if (taskTimeoutHandlersMap.containsKey(task.getName())) {
            logger.debug("Timeout task is already scheduled for task {}", task.getName());
            return;
        }

        final long timeoutTaskTime = task.getSubmittedAt() +
                DateTimeUtil.resolveDuration(task.getMaxExecutionTime());
        final long currentTimeMillis = System.currentTimeMillis();

        final TimeoutTask timeoutTask = new TimeoutTask(task);
        if (timeoutTaskTime < currentTimeMillis) {
            // submit timeout task now
            scheduledExecutorService.submit(timeoutTask);
        } else {
            logger.info("Initializing timeout task for task {}, scheduled at {}", task.getName(), timeoutTaskTime);
            final ScheduledFuture<?> timeoutTaskFuture =
                    scheduledExecutorService.schedule(timeoutTask, timeoutTaskTime - currentTimeMillis, MILLISECONDS);
            taskTimeoutHandlersMap.put(task.getName(), timeoutTaskFuture);
        }
    }

    private void resolveCreatedTasks() {
        final List<Task> tasks = taskProvider.getTasks(Collections.singletonList(CREATED));
        tasks.sort(comparing(Task::getCreatedAt));
        tasks.forEach(this::resolve);
    }

    @Override
    public void start() {
        logger.info("Starting task scheduler service");
        final long pollInterval = DateTimeUtil.resolveDuration(consumerConfig.getPollInterval());
        scheduledExecutorService.scheduleAtFixedRate(this::consumeTaskStatus, pollInterval, pollInterval, MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(this::deleteStaleTasks, TASK_PURGE_INTERVAL, TASK_PURGE_INTERVAL, HOURS);
    }

    private void consumeTaskStatus() {
        final List<String> tasksStatus = consumer.poll(statusQueue);
        for (String taskStatusAsString : tasksStatus) {
            try {
                final TaskUpdate taskUpdate = MAPPER.readValue(taskStatusAsString, TaskUpdate.class);
                updateStatus(taskUpdate.getTaskId(), taskUpdate.getStatus(),
                        taskUpdate.getStatusMessage(), taskUpdate.getContext());
            } catch (IOException e) {
                logger.error("Error parsing task status message {}", taskStatusAsString, e);
            }
        }
    }

    /**
     * deletes all the stale tasks from memory older than task purge interval
     */
    void deleteStaleTasks() {
        taskProvider.removeStaleTasks(HOURS.toMillis(TASK_PURGE_INTERVAL));
    }

    /**
     * register a listener to receive task status change notifications
     *
     * @param statusChangeListener
     */
    public void registerListener(TaskStatusChangeListener statusChangeListener) {
        statusChangeListeners.add(statusChangeListener);
    }

    /**
     * deregister a task status change listener
     *
     * @param statusChangeListener
     */
    public void deregisterListener(TaskStatusChangeListener statusChangeListener) {
        statusChangeListeners.remove(statusChangeListener);
    }

    synchronized void schedule(Task task) {
        logger.info("Received request to schedule task: {}", task);
        taskProvider.add(task);
        resolve(task);
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

    private void updateStatus(TaskId taskId, Status status, String statusMessage) {
        updateStatus(taskId, status, statusMessage, null);
    }

    private void updateStatus(TaskId taskId, Status status, String statusMessage,
                              Map<String, Object> context) {
        final Task task = taskProvider.getTask(taskId);
        if (task == null) {
            logger.error("No task found with id {}", taskId);
            return;
        }
        updateStatus(task, status, statusMessage, context);
    }

    private void updateStatus(Task task, Status status, String statusMessage, Map<String, Object> context) {
        logger.info("Received request to update status of task {} to {} " +
                "with status message {}", task, status, statusMessage);
        Status currentStatus = task.getStatus();
        if (!isValidTransition(currentStatus, status)) {
            logger.error("Invalid state transition for task {} from status {}, to {}", task, task.getStatus(), status);
            return;
        }
        MutableTask mutableTask = (MutableTask) task;
        mutableTask.setStatus(status);
        mutableTask.setStatusMessage(statusMessage);
        mutableTask.setContext(context);
        switch (status) {
            case SUBMITTED:
                mutableTask.setSubmittedAt(System.currentTimeMillis());
                break;
            case SUCCESSFUL:
            case FAILED:
                mutableTask.setCompletedAt(System.currentTimeMillis());
                break;
        }
        try {
            TaskService.getService().update(task);
        } catch (ServiceException e) {
            logger.error("Error updating status of task {} to {} with status message {}",
                    task, status, statusMessage, e);
        }
        notifyListeners(task, currentStatus, status);
        handleTaskStatusChange(task, status);
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

    private void notifyListeners(Task task, Status from, Status to) {
        statusChangeListeners.forEach(listener -> {
            try {
                listener.statusChanged(task, from, to);
            } catch (Exception e) {
                logger.error("error notifying task status change from {}, to {} for task {}",
                        from, to, task, e);
            }
        });
    }

    private void handleTaskStatusChange(Task task, Status status) {
        switch (status) {
            case CREATED:
                break;
            case WAITING:
                scheduleReadyTasks();
                break;
            case SCHEDULED:
                break;
            case SUBMITTED:
                createTimeoutTask(task);
                break;
            case FAILED:
                markDependentTasksAsFailed(task);
                // do not break
            case SUCCESSFUL:
                final ScheduledFuture<?> taskTimeoutFuture = taskTimeoutHandlersMap.remove(task.getName());
                if (taskTimeoutFuture != null) {
                    taskTimeoutFuture.cancel(false);
                }
                // If the task is finished (reached terminal state), proceed to schedule the next set of tasks
                scheduleReadyTasks();
                break;
        }
    }

    private void markDependentTasksAsFailed(Task task) {
        for (Task dependentTask : taskProvider.getDependentTasks(task)) {
            updateStatus(dependentTask, FAILED, FAILED_TO_RESOLVE_DEPENDENCY);
        }
    }

    /**
     * submit tasks ready for execution to queue
     */
    private synchronized void scheduleReadyTasks() {
        if (isInitialized) {
            final List<Task> readyTasks = taskProvider.getReadyTasks();
            for (Task task : readyTasks) {
                try {
                    // update task context from the tasks it depends on before scheduling
                    updateTaskContext(task);
                    producer.send(task.getType(), MAPPER.writeValueAsString(task));
                    updateStatus(task, SCHEDULED, null);
                } catch (Exception e) {
                    logger.error("Error submitting task {} to queue", task, e);
                    updateStatus(task, FAILED, TASK_SUBMISSION_FAILED);
                }
            }
        }
    }

    /**
     * updates the task properties with the context from the tasks it depends on.
     *
     * @param task
     */
    private void updateTaskContext(Task task) {
        final List<String> dependsOn = task.getDependsOn();
        final Map<String, Object> dependentTaskContext = new LinkedHashMap<>();
        for (String dependentTaskName : dependsOn) {
            // sort the tasks based on creation time and update the context from the latest task
            TaskId dependentTaskId = MutableTaskId.build(dependentTaskName, task.getJob(), task.getNamespace());
            Task dependentTask = taskProvider.getTask(dependentTaskId);
            if (dependentTask != null) {
                if (dependentTask.getContext() != null && !dependentTask.getContext().isEmpty()) {
                    dependentTask.getContext().forEach((key, value) ->
                            dependentTaskContext.put(dependentTask.getName() + "." + key, value));
                }
            }
        }
        updateTaskProperties(task, dependentTaskContext);
    }

    /**
     * update task properties from the dependent task context
     * <p>
     * dependent task context map is of the form
     * ${dependentTaskName}.key=value
     * </p>
     *
     * @param task
     * @param dependentTaskContext
     */
    // used in junit
    void updateTaskProperties(Task task, Map<String, Object> dependentTaskContext) {
        if (dependentTaskContext == null || dependentTaskContext.isEmpty()) {
            return;
        }

        final Map<String, Object> modifiedTaskProperties = new HashMap<>();
        task.getProperties().forEach((key, value) -> {
            if (value instanceof String && ((String) value).startsWith("${") && ((String) value).endsWith("}")) {
                final String valueToReplace = ((String) value).substring(2, ((String) value).length() - 1);
                if (dependentTaskContext.containsKey(valueToReplace)) {
                    modifiedTaskProperties.put(key, dependentTaskContext.get(valueToReplace));
                } else if (valueToReplace.startsWith("*") && valueToReplace.length() > 2) {
                    final String propertyToReplace = valueToReplace.substring(2);
                    dependentTaskContext.keySet().forEach(contextKey -> {
                        if (contextKey.substring(contextKey.indexOf(".") + 1).equals(propertyToReplace)) {
                            modifiedTaskProperties.put(key, dependentTaskContext.get(contextKey));
                        }
                    });
                } else {
                    // no dynamic property found to replace, setting it to null
                    logger.error("No dynamic property found in dependent task context to replace key: {}," +
                            " setting it to null", key);
                    modifiedTaskProperties.put(key, null);
                }
            } else {
                // copy the remaining key value pair as it is from current task properties
                modifiedTaskProperties.put(key, value);
            }
        });

        dependentTaskContext.forEach((key, value) -> {
            if (!modifiedTaskProperties.containsKey(key.substring(key.indexOf(".") + 1))) {
                modifiedTaskProperties.put(key.substring(key.indexOf(".") + 1), value);
            }
        });
        ((MutableTask) task).setProperties(modifiedTaskProperties);
    }

    // used in junit
    TaskProvider getTaskProvider() {
        return taskProvider;
    }

    // used in junit
    Consumer getConsumer() {
        return consumer;
    }

    // used in junit
    Producer getProducer() {
        return producer;
    }

    @Override
    public void stop() {
        logger.info("Stopping task scheduler service");
        if (consumer != null) {
            consumer.close();
        }
        try {
            scheduledExecutorService.shutdown();
            scheduledExecutorService.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error stopping thread pool", e);
        }
        if (producer != null) {
            producer.close();
        }
    }

    private class TimeoutTask implements Runnable {
        private final Task task;

        TimeoutTask(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            logger.info("Task {} has timed out, marking task as failed", task);
            updateStatus(task, FAILED, TIMED_OUT);
            final TimeoutPolicy timeoutPolicy = timeoutPolicyMap.get(task.getTimeoutPolicy());
            if (timeoutPolicy != null) {
                logger.info("Applying timeout policy {} on task {}", timeoutPolicy, task);
                timeoutPolicy.handle(task);
            }
        }
    }
}
