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
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class WorkflowService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowService.class);

    private final StoreConfig storeConfig;
    private WorkflowStore workflowStore;

    public WorkflowService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static WorkflowService getService() {
        return (WorkflowService) ServiceProvider.getService(WorkflowService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        logger.info("Initializing workflow service");
        workflowStore = (WorkflowStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {
        logger.info("Starting workflow service");
    }

    public void add(Workflow workflow) {
        logger.debug("Received request to add workflow {}", workflow);
        workflowStore.store(workflow);
    }

    public List<Workflow> get(String namespace) {
        logger.debug("Received request to get all workflows under namespace {}", namespace);
        return workflowStore.load(namespace);
    }

    public Workflow get(WorkflowId workflowId) {
        logger.debug("Received request to get workflow {}", workflowId);
        return workflowStore.load(workflowId);
    }

    public List<Workflow> get(String namespace, int numberOfDays) {
        logger.debug("Received request to get all workflows under namespace {} submitted in last {} number of days",
                namespace, numberOfDays);
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        return workflowStore.load(namespace, createdAfter, createdBefore);
    }

    public List<Workflow> get(String name, String namespace, int numberOfDays) {
        logger.debug("Received request to get all workflows with name {} under namespace {} submitted in last {} number of days",
                name, namespace, numberOfDays);
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        return workflowStore.loadByName(name, namespace, createdAfter, createdBefore);
    }

    public List<Workflow> get(String name, String trigger,
                              String namespace, int numberOfDays) {
        logger.debug("Received request to get all workflows with name {}, trigger {} under namespace {} submitted " +
                "in last {} number of days", name, trigger, namespace, numberOfDays);
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        return workflowStore.loadByNameAndTrigger(name, trigger, namespace, createdAfter, createdBefore);
    }

    public List<Task> getWorkflowTasks(WorkflowId workflowId) {
        logger.debug("Received request to get all tasks executed for workflow {}", workflowId);
        return TaskService.getService().get(workflowId.getId(), workflowId.getNamespace());
    }

    public void update(Workflow workflow) {
        logger.debug("Received request to update workflow to {}", workflow);
        workflowStore.update(workflow);
    }

    public void delete(WorkflowId workflowId) {
        logger.debug("Received request to delete workflow {}", workflowId);
        workflowStore.delete(workflowId);
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow service");
        workflowStore.stop();
    }
}