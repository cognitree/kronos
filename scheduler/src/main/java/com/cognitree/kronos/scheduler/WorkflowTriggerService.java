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
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.NamespaceId;
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

    public void add(WorkflowTrigger workflowTrigger) throws SchedulerException, ServiceException, ValidationException {
        logger.debug("Received request to add workflow trigger {}", workflowTrigger);
        validateNamespace(workflowTrigger.getNamespace());
        validate(workflowTrigger.getWorkflow(), workflowTrigger.getNamespace());
        try {
            workflowTriggerStore.store(workflowTrigger);
            final Workflow workflow = WorkflowService.getService()
                    .get(WorkflowId.build(workflowTrigger.getWorkflow(), workflowTrigger.getNamespace()));
            WorkflowSchedulerService.getService().schedule(workflow, workflowTrigger);
        } catch (StoreException e) {
            logger.error("unable to add workflow trigger {}", workflowTrigger, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<WorkflowTrigger> get(String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all workflow triggers under namespace {}", namespace);
        validateNamespace(namespace);
        try {
            return workflowTriggerStore.load(namespace);
        } catch (StoreException e) {
            logger.error("unable to get all workflow triggers under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public WorkflowTrigger get(WorkflowTriggerId workflowTriggerId) throws ServiceException, ValidationException {
        logger.debug("Received request to get workflow trigger with id {}", workflowTriggerId);
        validateNamespace(workflowTriggerId.getNamespace());
        validate(workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace());
        try {
            return workflowTriggerStore.load(workflowTriggerId);
        } catch (StoreException e) {
            logger.error("unable to get workflow trigger with id {}", workflowTriggerId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<WorkflowTrigger> get(String workflowName, String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all workflow triggers for workflow {} under namespace {}",
                workflowName, namespace);
        validateNamespace(namespace);
        validate(workflowName, namespace);
        try {
            return workflowTriggerStore.loadByWorkflowName(workflowName, namespace);
        } catch (StoreException e) {
            logger.error("unable to get all workflow triggers for workflow {} under namespace {}",
                    workflowName, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void update(WorkflowTrigger workflowTrigger) throws SchedulerException, ServiceException, ValidationException {
        logger.debug("Received request to update workflow trigger to {}", workflowTrigger);
        validateNamespace(workflowTrigger.getNamespace());
        try {
            workflowTriggerStore.update(workflowTrigger);
            WorkflowSchedulerService.getService().delete(workflowTrigger);
            final Workflow workflow = WorkflowService.getService()
                    .get(WorkflowId.build(workflowTrigger.getWorkflow(), workflowTrigger.getNamespace()));
            WorkflowSchedulerService.getService().schedule(workflow, workflowTrigger);
        } catch (StoreException e) {
            logger.error("unable to update workflow trigger to {}", workflowTrigger, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void delete(WorkflowTriggerId workflowTriggerId) throws SchedulerException, ServiceException, ValidationException {
        logger.debug("Received request to delete workflow trigger {}", workflowTriggerId);
        validateNamespace(workflowTriggerId.getNamespace());
        try {
            workflowTriggerStore.delete(workflowTriggerId);
            WorkflowSchedulerService.getService().delete(workflowTriggerId);
        } catch (StoreException e) {
            logger.error("unable to delete workflow trigger {}", workflowTriggerId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    private void validate(String workflowName, String namespace) throws ServiceException, ValidationException {
        WorkflowId workflowId = WorkflowId.build(workflowName, namespace);
        if (WorkflowService.getService().get(workflowId) == null) {
            logger.error("No workflow exists with name {} under namespace {}", workflowName, namespace);
            throw new ValidationException("No workflow exists with name " + workflowName + " under namespace " + namespace);
        }
    }

    private void validateNamespace(String name) throws ValidationException, ServiceException {
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            throw new ValidationException("no namespace exists with name " + name);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow trigger service");
        workflowTriggerStore.stop();
    }
}