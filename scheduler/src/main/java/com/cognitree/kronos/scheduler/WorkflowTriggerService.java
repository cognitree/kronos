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
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.model.WorkflowTrigger;
import com.cognitree.kronos.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WorkflowTriggerService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowSchedulerService.class);

    private final StoreConfig storeConfig;
    private WorkflowTriggerStore workflowTriggerStore;

    public WorkflowTriggerService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static WorkflowTriggerService getService() {
        return (WorkflowTriggerService) ServiceProvider.getService(WorkflowTriggerService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        logger.info("Initializing workflow trigger service");
        workflowTriggerStore = (WorkflowTriggerStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowTriggerStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {
        logger.info("Starting workflow trigger service");
    }

    public void add(WorkflowTrigger workflowTrigger) throws SchedulerException, ServiceException {
        logger.debug("Received request to add workflow trigger {}", workflowTrigger);
        final Workflow workflow = WorkflowService.getService()
                .get(WorkflowId.build(workflowTrigger.getWorkflow(), workflowTrigger.getNamespace()));
        WorkflowSchedulerService.getService().schedule(workflow, workflowTrigger);
        try {
            workflowTriggerStore.store(workflowTrigger);
        } catch (StoreException e) {
            logger.error("unable to add workflow trigger {}", workflowTrigger, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<WorkflowTrigger> get(String namespace) throws ServiceException {
        logger.debug("Received request to get all workflow triggers under namespace {}", namespace);
        try {
            return workflowTriggerStore.load(namespace);
        } catch (StoreException e) {
            logger.error("unable to get all workflow triggers under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public WorkflowTrigger get(WorkflowTriggerId workflowTriggerId) throws ServiceException {
        logger.debug("Received request to get workflow trigger with id {}", workflowTriggerId);
        try {
            return workflowTriggerStore.load(workflowTriggerId);
        } catch (StoreException e) {
            logger.error("unable to get workflow trigger with id {}", workflowTriggerId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<WorkflowTrigger> get(String workflowName, String namespace) throws ServiceException {
        logger.debug("Received request to get all workflow triggers for workflow {} under namespace {}",
                workflowName, namespace);
        try {
            return workflowTriggerStore.loadByWorkflowName(workflowName, namespace);
        } catch (StoreException e) {
            logger.error("unable to get all workflow triggers for workflow {} under namespace {}",
                    workflowName, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void update(WorkflowTrigger workflowTrigger) throws SchedulerException, ServiceException {
        logger.debug("Received request to update workflow trigger to {}", workflowTrigger);
        WorkflowSchedulerService.getService().delete(workflowTrigger);
        final Workflow workflow = WorkflowService.getService()
                .get(WorkflowId.build(workflowTrigger.getWorkflow(), workflowTrigger.getNamespace()));
        WorkflowSchedulerService.getService().schedule(workflow, workflowTrigger);
        try {
            workflowTriggerStore.update(workflowTrigger);
        } catch (StoreException e) {
            logger.error("unable to update workflow trigger to {}", workflowTrigger, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void delete(WorkflowTriggerId workflowTriggerId) throws SchedulerException, ServiceException {
        logger.debug("Received request to delete workflow trigger {}", workflowTriggerId);
        WorkflowSchedulerService.getService().delete(workflowTriggerId);
        try {
            workflowTriggerStore.delete(workflowTriggerId);
        } catch (StoreException e) {
            logger.error("unable to delete workflow trigger {}", workflowTriggerId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow trigger service");
        workflowTriggerStore.stop();
    }
}