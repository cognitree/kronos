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
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.model.Workflow.WorkflowTask;
import com.cognitree.kronos.scheduler.store.StoreException;
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

public class TaskService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

    private final Set<TaskStatusChangeListener> statusChangeListeners = new HashSet<>();
    private TaskStore taskStore;

    public TaskService(TaskStore taskStore) {
        this.taskStore = taskStore;
    }

    public static TaskService getService() {
        return (TaskService) ServiceProvider.getService(TaskService.class.getSimpleName());
    }

    public void init() {
        logger.info("Initializing task service");
    }

    public void start() {
        logger.info("Starting task service");
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
    public void deregisterListener(TaskStatusChangeListener statusChangeListener) {
        statusChangeListeners.remove(statusChangeListener);
    }

    public Task create(String jobId, WorkflowTask workflowTask, String namespace) throws ServiceException {
        logger.debug("Received request to create task for job {} from workflow task {} under namespace {}",
                jobId, workflowTask, namespace);
        MutableTask task = new MutableTask();
        task.setName(UUID.randomUUID().toString());
        task.setJob(jobId);
        task.setName(workflowTask.getName());
        task.setNamespace(namespace);
        task.setType(workflowTask.getType());
        task.setMaxExecutionTime(workflowTask.getMaxExecutionTime());
        task.setTimeoutPolicy(workflowTask.getTimeoutPolicy());
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


    public List<Task> get(String namespace) throws ServiceException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        try {
            final List<Task> tasks = taskStore.loadByStatusIn(namespace);
            return tasks == null ? Collections.emptyList() : tasks;
        } catch (StoreException e) {
            logger.error("unable to get all tasks under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public Task get(TaskId taskId) throws ServiceException {
        logger.debug("Received request to get task {}", taskId);
        try {
            return taskStore.load(taskId);
        } catch (StoreException e) {
            logger.error("unable to get task {}", taskId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Task> get(String jobId, String namespace) throws ServiceException {
        logger.debug("Received request to get all tasks with job id {} under namespace {}",
                jobId, namespace);
        try {
            final List<Task> tasks = taskStore.loadByJobId(jobId, namespace);
            return tasks == null ? Collections.emptyList() : tasks;
        } catch (StoreException e) {
            logger.error("unable to get all tasks with job id {} under namespace {}",
                    jobId, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Task> get(List<Status> statuses, String namespace) throws ServiceException {
        logger.debug("Received request to get all tasks having status in {} under namespace {}",
                statuses, namespace);
        try {
            final List<Task> tasks = taskStore.loadByStatus(statuses, namespace);
            return tasks == null ? Collections.emptyList() : tasks;
        } catch (StoreException e) {
            logger.error("unable to get all tasks having status in {} under namespace {}",
                    statuses, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void update(Task task) throws ServiceException {
        logger.debug("Received request to update task {}", task);
        try {
            taskStore.update(task);
        } catch (StoreException e) {
            logger.error("unable to update task {}", task, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void updateStatus(Task task, Status status, String statusMessage, Map<String, Object> context) throws ServiceException {
        Status currentStatus = task.getStatus();
        if (!isValidTransition(currentStatus, status)) {
            logger.error("Invalid state transition for task {} from status {}, to {}", task, currentStatus, status);
            throw new ServiceException("Invalid state transition from " + currentStatus + " to " + status);
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
            taskStore.update(mutableTask);
        } catch (StoreException e) {
            logger.error("unable to update task {}", task, e);
            throw new ServiceException(e.getMessage());
        }
        notifyListeners(mutableTask, currentStatus, status);
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

    public void delete(TaskId taskId) throws ServiceException {
        logger.debug("Received request to delete task {}", taskId);
        try {
            taskStore.delete(taskId);
        } catch (StoreException e) {
            logger.error("unable to delete task {}", taskId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void stop() {
        logger.info("Stopping task service");
    }
}
