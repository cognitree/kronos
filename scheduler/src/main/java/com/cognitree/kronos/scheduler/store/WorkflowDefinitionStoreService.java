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

import com.cognitree.kronos.ReviewPending;
import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;

import java.util.List;

@ReviewPending
public class WorkflowDefinitionStoreService implements StoreService<WorkflowDefinition, WorkflowDefinitionId> {

    private final WorkflowDefinitionStoreConfig workflowDefinitionStoreConfig;
    private WorkflowDefinitionStore workflowDefinitionStore;

    public WorkflowDefinitionStoreService(WorkflowDefinitionStoreConfig workflowDefinitionStoreConfig) {
        this.workflowDefinitionStoreConfig = workflowDefinitionStoreConfig;
    }

    public static WorkflowDefinitionStoreService getService() {
        return (WorkflowDefinitionStoreService) StoreServiceProvider.getStoreService(WorkflowDefinitionStoreService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        workflowDefinitionStore = (WorkflowDefinitionStore) Class.forName(workflowDefinitionStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowDefinitionStore.init(workflowDefinitionStoreConfig.getConfig());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        workflowDefinitionStore.stop();
    }

    public void store(WorkflowDefinition workflowDefinition) {
        workflowDefinitionStore.store(workflowDefinition);
    }

    public List<WorkflowDefinition> load() {
        return workflowDefinitionStore.load();
    }

    public WorkflowDefinition load(WorkflowDefinitionId workflowDefinitionId) {
        return workflowDefinitionStore.load(workflowDefinitionId);
    }

    public void update(WorkflowDefinition workflowDefinition) {
        workflowDefinitionStore.store(workflowDefinition);
    }

    public void delete(WorkflowDefinitionId workflowDefinitionId) {
        workflowDefinitionStore.delete(workflowDefinitionId);
    }
}
