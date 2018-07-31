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

import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;

import java.util.List;

public class TaskDefinitionStoreService implements StoreService<TaskDefinition, TaskDefinitionId> {

    private final TaskDefinitionStoreConfig taskDefinitionStoreConfig;
    private TaskDefinitionStore taskDefinitionStore;

    public TaskDefinitionStoreService(TaskDefinitionStoreConfig taskDefinitionStoreConfig) {
        this.taskDefinitionStoreConfig = taskDefinitionStoreConfig;
    }

    public static TaskDefinitionStoreService getService() {
        return (TaskDefinitionStoreService) StoreServiceProvider.getStoreService(TaskDefinitionStoreService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        taskDefinitionStore = (TaskDefinitionStore) Class.forName(taskDefinitionStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        taskDefinitionStore.init(taskDefinitionStoreConfig.getConfig());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        taskDefinitionStore.stop();
    }

    public List<TaskDefinition> load() {
        return taskDefinitionStore.load();
    }

    public TaskDefinition load(TaskDefinitionId taskDefinitionId) {
        return taskDefinitionStore.load(taskDefinitionId);
    }

    public void store(TaskDefinition taskDefinition) {
        taskDefinitionStore.store(taskDefinition);
    }

    public void update(TaskDefinition taskDefinition) {
        taskDefinitionStore.store(taskDefinition);
    }

    public void delete(TaskDefinitionId taskDefinitionId) {
        taskDefinitionStore.delete(taskDefinitionId);
    }

}
