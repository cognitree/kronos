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
import com.cognitree.kronos.model.definitions.WorkflowTrigger;
import com.cognitree.kronos.model.definitions.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WorkflowTriggerService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowSchedulerService.class);

    private final StoreConfig storeConfig;
    private WorkflowTriggerStore workflowStore;

    public WorkflowTriggerService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static WorkflowTriggerService getService() {
        return (WorkflowTriggerService) ServiceProvider.getService(WorkflowTriggerService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        logger.info("Initializing workflow trigger service");
        workflowStore = (WorkflowTriggerStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {
        logger.info("Starting workflow trigger service");
    }

    public void add(WorkflowTrigger workflowTrigger) throws SchedulerException {
        logger.debug("Received request to add workflow trigger {}", workflowTrigger);
        WorkflowSchedulerService.getService().schedule(workflowTrigger);
        workflowStore.store(workflowTrigger);
    }

    public List<WorkflowTrigger> get(String namespace) {
        logger.debug("Received request to get all workflow trigger under namespace {}", namespace);
        return workflowStore.load(namespace);
    }

    public WorkflowTrigger get(WorkflowTriggerId workflowTriggerId) {
        logger.debug("Received request to get all workflow trigger {}", workflowTriggerId);
        return workflowStore.load(workflowTriggerId);
    }

    public List<WorkflowTrigger> get(String workflowName, String namespace) {
        logger.debug("Received request to get all workflow trigger for workflow {} under namespace {}",
                workflowName, namespace);
        return workflowStore.loadByWorkflowName(workflowName, namespace);
    }

    public void update(WorkflowTrigger workflowTrigger) throws SchedulerException {
        logger.debug("Received request to update workflow trigger to {}", workflowTrigger);
        WorkflowSchedulerService.getService().delete(workflowTrigger);
        WorkflowSchedulerService.getService().schedule(workflowTrigger);
        workflowStore.update(workflowTrigger);
    }

    public void delete(WorkflowTriggerId workflowTriggerId) throws SchedulerException {
        logger.debug("Received request to delete workflow trigger {}", workflowTriggerId);
        WorkflowSchedulerService.getService().delete(workflowTriggerId);
        workflowStore.delete(workflowTriggerId);
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow trigger service");
        workflowStore.stop();
    }
}