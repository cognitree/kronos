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
import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.TaskDefinitionStore;

import java.util.List;

public class TaskDefinitionService implements Service {

    private final StoreConfig storeConfig;
    private TaskDefinitionStore taskDefinitionStore;

    public TaskDefinitionService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static TaskDefinitionService getService() {
        return (TaskDefinitionService) ServiceProvider.getService(TaskDefinitionService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        taskDefinitionStore = (TaskDefinitionStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        taskDefinitionStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {

    }

    public List<TaskDefinition> get() {
        return taskDefinitionStore.load();
    }

    public TaskDefinition get(TaskDefinitionId taskDefinitionId) {
        return taskDefinitionStore.load(taskDefinitionId);
    }

    public void add(TaskDefinition taskDefinition) {
        taskDefinitionStore.store(taskDefinition);
    }

    public void update(TaskDefinition taskDefinition) {
        taskDefinitionStore.store(taskDefinition);
    }

    public void delete(TaskDefinitionId taskDefinitionId) {
        taskDefinitionStore.delete(taskDefinitionId);
    }

    @Override
    public void stop() {
        taskDefinitionStore.stop();
    }

}
