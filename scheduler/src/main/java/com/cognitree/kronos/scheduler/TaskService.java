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
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.TaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TaskService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

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

    }

    public List<Task> get(String namespace) throws ServiceException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        try {
            return taskStore.loadByStatusIn(namespace);
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
            return taskStore.loadByJobId(jobId, namespace);
        } catch (StoreException e) {
            logger.error("unable to get all tasks with job id {} under namespace {}",
                    jobId, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Task> get(List<Task.Status> statuses, String namespace) throws ServiceException {
        logger.debug("Received request to get all tasks having status in {} under namespace {}",
                statuses, namespace);
        try {
            return taskStore.loadByStatus(statuses, namespace);
        } catch (StoreException e) {
            logger.error("unable to get all tasks having status in {} under namespace {}",
                    statuses, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void add(Task task) throws ServiceException {
        logger.debug("Received request to add task {}", task);
        try {
            taskStore.store(task);
        } catch (StoreException e) {
            logger.error("unable to add task {}", task, e);
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
