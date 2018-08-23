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
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinition.WorkflowTask;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;

import java.util.List;

public class WorkflowStoreService implements StoreService<Workflow, WorkflowId> {

    private final WorkflowStoreConfig workflowStoreConfig;
    private WorkflowStore workflowStore;

    public WorkflowStoreService(WorkflowStoreConfig workflowStoreConfig) {
        this.workflowStoreConfig = workflowStoreConfig;
    }

    public static WorkflowStoreService getService() {
        return (WorkflowStoreService) StoreServiceProvider.getStoreService(WorkflowStoreService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        workflowStore = (WorkflowStore) Class.forName(workflowStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowStore.init(workflowStoreConfig.getConfig());
    }

    @Override
    public void start() {

    }

    @Override
    public void store(Workflow workflow) {
        workflowStore.store(workflow);
    }

    public List<Workflow> load(String namespace) {
        return workflowStore.load(namespace);
    }

    @Override
    public Workflow load(WorkflowId workflowId) {
        return workflowStore.load(workflowId);
    }

    public List<Workflow> load(String namespace, long createdAfter, long createdBefore) {
        return workflowStore.load(namespace, createdAfter, createdBefore);
    }

    public List<Workflow> loadByName(String name, String namespace, long createdAfter, long createdBefore) {
        return workflowStore.loadByName(name, namespace, createdAfter, createdBefore);
    }

    public List<Task> getWorkflowTasks(WorkflowId workflowId) {
        return TaskStoreService.getService().loadByWorkflowId(workflowId.getId(), workflowId.getNamespace());
    }

    public List<WorkflowTask> getWorkflowTaskDefs(Workflow workflow) {
        WorkflowDefinitionId workflowDefinitionId =
                WorkflowDefinitionId.create(workflow.getName(), workflow.getNamespace());
        final WorkflowDefinition workflowDefinition =
                WorkflowDefinitionStoreService.getService().load(workflowDefinitionId);
        return workflowDefinition.getTasks();
    }

    @Override
    public void update(Workflow workflow) {
        workflowStore.update(workflow);
    }

    @Override
    public void delete(WorkflowId workflowId) {
        workflowStore.delete(workflowId);
    }

    @Override
    public void stop() {
        workflowStore.stop();
    }
}