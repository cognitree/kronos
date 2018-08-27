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
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.TaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TaskService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

    private StoreConfig storeConfig;
    private TaskStore taskStore;

    public TaskService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static TaskService getService() {
        return (TaskService) ServiceProvider.getService(TaskService.class.getSimpleName());
    }

    public void init() throws Exception {
        logger.info("Initializing task service");
        taskStore = (TaskStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        taskStore.init(storeConfig.getConfig());
    }

    public void start() {
        logger.info("Starting task service");

    }

    public List<Task> get(String namespace) {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        return taskStore.load(namespace);
    }

    public Task get(TaskId taskId) {
        logger.debug("Received request to get task {}", taskId);
        return taskStore.load(taskId);
    }

    public List<Task> get(String taskName, String workflowId, String namespace) {
        logger.debug("Received request to get all tasks with name {} part of workflow {} under namespace {}",
                taskName, workflowId, namespace);
        return taskStore.loadByNameAndWorkflowId(taskName, workflowId, namespace);
    }

    public List<Task> get(String workflowId, String namespace) {
        logger.debug("Received request to get all tasks with workflow id {} under namespace {}",
                workflowId, namespace);
        return taskStore.loadByWorkflowId(workflowId, namespace);
    }

    public List<Task> get(List<Task.Status> statuses, String namespace) {
        logger.debug("Received request to get all tasks having status in {} under namespace {}",
                statuses, namespace);
        return taskStore.load(statuses, namespace);
    }

    public void add(Task task) {
        logger.debug("Received request to add task {}", task);
        taskStore.store(task);
    }

    public void update(Task task) {
        logger.debug("Received request to update task {}", task);
        taskStore.update(task);
    }

    public void delete(TaskId taskId) {
        logger.debug("Received request to delete task {}", taskId);
        taskStore.delete(taskId);
    }

    public void stop() {
        logger.info("Stopping task service");
        taskStore.stop();
    }
}
