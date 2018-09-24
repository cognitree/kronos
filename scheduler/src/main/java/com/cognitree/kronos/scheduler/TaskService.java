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
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.Workflow.WorkflowTask;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.TaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.cognitree.kronos.model.Task.Status.CREATED;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.model.Task.Status.WAITING;
import static com.cognitree.kronos.scheduler.ValidationError.JOB_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.NAMESPACE_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.TASK_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.WORKFLOW_NOT_FOUND;

public class TaskService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

    private final Set<TaskStatusChangeListener> statusChangeListeners = new HashSet<>();
    private TaskStore taskStore;

    public static TaskService getService() {
        return (TaskService) ServiceProvider.getService(TaskService.class.getSimpleName());
    }

    public void init() {
        logger.info("Initializing task service");
    }

    public void start() {
        logger.info("Starting task service");
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        taskStore = storeService.getTaskStore();
        ServiceProvider.registerService(this);
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
    public void deRegisterListener(TaskStatusChangeListener statusChangeListener) {
        statusChangeListeners.remove(statusChangeListener);
    }

    Task create(String namespace, WorkflowTask workflowTask, String jobId, String workflowName)
            throws ServiceException, ValidationException {
        logger.debug("Received request to create task from workflow task {} for job {}, workflow {} under namespace {}",
                workflowTask, jobId, workflowName, namespace);
        validateJob(namespace, jobId, workflowName);
        Task task = new Task();
        task.setName(UUID.randomUUID().toString());
        task.setJob(jobId);
        task.setWorkflow(workflowName);
        task.setName(workflowTask.getName());
        task.setNamespace(namespace);
        task.setType(workflowTask.getType());
        task.setMaxExecutionTimeInMs(workflowTask.getMaxExecutionTimeInMs());
        task.setDependsOn(workflowTask.getDependsOn());
        task.setProperties(workflowTask.getProperties());
        task.setCreatedAt(System.currentTimeMillis());
        try {
            taskStore.store(task);
        } catch (StoreException e) {
            logger.error("unable to add task {}", task, e);
            throw new ServiceException(e.getMessage());
        }
        return task;
    }


    public List<Task> get(String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        validateNamespace(namespace);
        try {
            final List<Task> tasks = taskStore.load(namespace);
            return tasks == null ? Collections.emptyList() : tasks;
        } catch (StoreException e) {
            logger.error("unable to get all tasks under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public Task get(TaskId taskId) throws ServiceException, ValidationException {
        logger.debug("Received request to get task {}", taskId);
        validateJob(taskId.getNamespace(), taskId.getJob(), taskId.getWorkflow());
        try {
            return taskStore.load(taskId);
        } catch (StoreException e) {
            logger.error("unable to get task {}", taskId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Task> get(String namespace, String jobId, String workflowName) throws ServiceException, ValidationException {
        logger.debug("Received request to get all tasks with job id {} for workflow {} under namespace {}",
                jobId, workflowName, namespace);
        validateJob(namespace, jobId, workflowName);
        try {
            final List<Task> tasks = taskStore.loadByJobIdAndWorkflowName(namespace, jobId, workflowName);
            return tasks == null ? Collections.emptyList() : tasks;
        } catch (StoreException e) {
            logger.error("unable to get all tasks with job id {} for workflow {} under namespace {}",
                    jobId, workflowName, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Task> get(String namespace, List<Status> statuses) throws ServiceException, ValidationException {
        logger.debug("Received request to get all tasks having status in {} under namespace {}",
                statuses, namespace);
        validateNamespace(namespace);
        try {
            final List<Task> tasks = taskStore.loadByStatus(namespace, statuses);
            return tasks == null ? Collections.emptyList() : tasks;
        } catch (StoreException e) {
            logger.error("unable to get all tasks having status in {} under namespace {}",
                    statuses, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public Map<Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore)
            throws ServiceException, ValidationException {
        logger.debug("Received request to count tasks by status under namespace {} created between {} to {}",
                namespace, createdAfter, createdBefore);
        validateNamespace(namespace);
        try {
            return taskStore.countByStatus(namespace, createdAfter, createdBefore);
        } catch (StoreException e) {
            logger.error("unable to count tasks by status under namespace {} created between {} to {}",
                    namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public Map<Status, Integer> countByStatus(String namespace, String workflowName, long createdAfter, long createdBefore)
            throws ServiceException, ValidationException {
        logger.debug("Received request to count tasks by status having workflow name {} under namespace {} created " +
                "between {} to {}", workflowName, namespace, createdAfter, createdBefore);
        validateWorkflow(namespace, workflowName);
        try {
            return taskStore.countByStatusForWorkflowName(namespace, workflowName, createdAfter, createdBefore);
        } catch (StoreException e) {
            logger.error("unable to count tasks by status having workflow name {} under namespace {} created " +
                    "between {} to {}", workflowName, namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage());
        }
    }

    void updateStatus(Task task, Status status, String statusMessage, Map<String, Object> context)
            throws ServiceException, ValidationException {
        try {
            if (taskStore.load(task) == null) {
                throw TASK_NOT_FOUND.createException(task.getName(), task.getJob(), task.getWorkflow(), task.getNamespace());
            }
            validateJob(task.getNamespace(), task.getJob(), task.getWorkflow());
            Status currentStatus = task.getStatus();
            if (!isValidTransition(currentStatus, status)) {
                logger.error("Invalid state transition for task {} from status {}, to {}", task, currentStatus, status);
                throw new ServiceException("Invalid state transition from " + currentStatus + " to " + status);
            }
            task.setStatus(status);
            task.setStatusMessage(statusMessage);
            task.setContext(context);
            switch (status) {
                case SUBMITTED:
                    task.setSubmittedAt(System.currentTimeMillis());
                    break;
                case SUCCESSFUL:
                case FAILED:
                    task.setCompletedAt(System.currentTimeMillis());
                    break;
            }
            taskStore.update(task);
            notifyListeners(task, currentStatus, status);
        } catch (StoreException e) {
            logger.error("unable to update task {}", task, e);
            throw new ServiceException(e.getMessage());
        }
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
                logger.error("error notifying task status change from {}, to {} for task {}", from, to, task, e);
            }
        });
    }

    public void delete(TaskId taskId) throws ServiceException, ValidationException {
        logger.debug("Received request to delete task {}", taskId);
        validateJob(taskId.getNamespace(), taskId.getJob(), taskId.getWorkflow());
        try {
            if (taskStore.load(taskId) == null) {
                throw TASK_NOT_FOUND.createException(taskId.getName(), taskId.getJob(), taskId.getWorkflow(), taskId.getNamespace());
            }
            taskStore.delete(taskId);
        } catch (StoreException e) {
            logger.error("unable to delete task {}", taskId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    private void validateNamespace(String name) throws ValidationException, ServiceException {
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            throw NAMESPACE_NOT_FOUND.createException(name);
        }
    }

    private void validateWorkflow(String namespace, String workflowName) throws ServiceException, ValidationException {
        final Workflow workflow = WorkflowService.getService().get(WorkflowId.build(namespace, workflowName));
        if (workflow == null) {
            logger.error("No workflow exists with name {} under namespace {}", workflowName, namespace);
            throw WORKFLOW_NOT_FOUND.createException(workflowName, namespace);
        }
    }

    private void validateJob(String namespace, String jobId, String workflowName) throws ServiceException, ValidationException {
        final Job job = JobService.getService().get(JobId.build(namespace, jobId, workflowName));
        if (job == null) {
            logger.error("No job exists with id {} for workflow {} under namespace {}", jobId, workflowName, namespace);
            throw JOB_NOT_FOUND.createException(jobId, workflowName, namespace);
        }
    }

    public void stop() {
        logger.info("Stopping task service");
    }
}
