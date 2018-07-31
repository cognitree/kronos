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

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;

import java.util.List;

public class TaskStoreService implements StoreService<Task, TaskId> {

    private TaskStoreConfig taskStoreConfig;
    private TaskStore taskStore;

    public TaskStoreService(TaskStoreConfig taskStoreConfig) {
        this.taskStoreConfig = taskStoreConfig;
    }

    public static TaskStoreService getService() {
        return (TaskStoreService) StoreServiceProvider.getStoreService(TaskStoreService.class.getSimpleName());
    }

    public void init() throws Exception {
        taskStore = (TaskStore) Class.forName(taskStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        taskStore.init(taskStoreConfig.getConfig());
    }

    public void start() {

    }

    public void stop() {
        taskStore.stop();
    }

    public List<Task> load() {
        return taskStore.load();
    }

    public Task load(TaskId taskId) {
        return taskStore.load(taskId);
    }

    public List<Task> loadByNameAndWorkflowId(String taskName, String workflowId, String namespace) {
        return taskStore.loadByNameAndWorkflowId(taskName, workflowId, namespace);
    }

    public List<Task> loadByWorkflowId(String workflowId, String namespace) {
        return taskStore.loadByWorkflowId(workflowId, namespace);
    }

    public List<Task> load(List<Task.Status> statuses, String namespace) {
        return taskStore.load(statuses, namespace);
    }

    public void store(Task taskDefinition) {
        taskStore.store(taskDefinition);
    }

    public void update(Task task) {
        taskStore.update(task);
    }

    public void delete(TaskId taskId) {
        taskStore.delete(taskId);
    }
}
