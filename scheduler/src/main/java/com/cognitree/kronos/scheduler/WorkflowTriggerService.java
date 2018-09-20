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
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.cognitree.kronos.scheduler.util.TriggerHelper;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;

import static com.cognitree.kronos.scheduler.ValidationError.*;
import static com.cognitree.kronos.scheduler.ValidationError.NAMESPACE_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.WORKFLOW_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.WORKFLOW_TRIGGER_ALREADY_EXISTS;
import static com.cognitree.kronos.scheduler.ValidationError.WORKFLOW_TRIGGER_NOT_FOUND;

public class WorkflowTriggerService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowSchedulerService.class);

    private WorkflowTriggerStore workflowTriggerStore;

    public static WorkflowTriggerService getService() {
        return (WorkflowTriggerService) ServiceProvider.getService(WorkflowTriggerService.class.getSimpleName());
    }

    @Override
    public void init() {
        logger.info("Initializing workflow trigger service");
    }

    @Override
    public void start() {
        logger.info("Starting workflow trigger service");
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        workflowTriggerStore = storeService.getWorkflowTriggerStore();
        ServiceProvider.registerService(this);
    }

    public void add(WorkflowTrigger workflowTrigger) throws SchedulerException, ServiceException, ValidationException {
        logger.debug("Received request to add workflow trigger {}", workflowTrigger);
        validateTrigger(workflowTrigger);
        validateWorkflow(workflowTrigger.getWorkflow(), workflowTrigger.getNamespace());
        try {
            if (workflowTriggerStore.load(workflowTrigger) != null) {
                throw WORKFLOW_TRIGGER_ALREADY_EXISTS.createException(workflowTrigger.getName(),
                        workflowTrigger.getWorkflow(), workflowTrigger.getNamespace());
            }
            workflowTriggerStore.store(workflowTrigger);
            final Workflow workflow = WorkflowService.getService()
                    .get(WorkflowId.build(workflowTrigger.getWorkflow(), workflowTrigger.getNamespace()));
            WorkflowSchedulerService.getService().schedule(workflow, workflowTrigger);
        } catch (StoreException | ParseException e) {
            logger.error("unable to add workflow trigger {}", workflowTrigger, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<WorkflowTrigger> get(String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all workflow triggers under namespace {}", namespace);
        validateNamespace(namespace);
        try {
            final List<WorkflowTrigger> workflowTriggers = workflowTriggerStore.load(namespace);
            return workflowTriggers == null ? Collections.emptyList() : workflowTriggers;
        } catch (StoreException e) {
            logger.error("unable to get all workflow triggers under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public WorkflowTrigger get(WorkflowTriggerId workflowTriggerId) throws ServiceException, ValidationException {
        logger.debug("Received request to get workflow trigger with id {}", workflowTriggerId);
        validateWorkflow(workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace());
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
        validateWorkflow(workflowName, namespace);
        try {
            final List<WorkflowTrigger> workflowTriggers = workflowTriggerStore.loadByWorkflowName(workflowName, namespace);
            return workflowTriggers == null ? Collections.emptyList() : workflowTriggers;
        } catch (StoreException e) {
            logger.error("unable to get all workflow triggers for workflow {} under namespace {}",
                    workflowName, namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void delete(WorkflowTriggerId workflowTriggerId) throws SchedulerException, ServiceException, ValidationException {
        logger.debug("Received request to delete workflow trigger {}", workflowTriggerId);
        validateNamespace(workflowTriggerId.getNamespace());
        try {
            if (workflowTriggerStore.load(workflowTriggerId) == null) {
                throw WORKFLOW_TRIGGER_NOT_FOUND.createException(workflowTriggerId.getName(),
                        workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace());
            }
            workflowTriggerStore.delete(workflowTriggerId);
            WorkflowSchedulerService.getService().delete(workflowTriggerId);
        } catch (StoreException e) {
            logger.error("unable to delete workflow trigger {}", workflowTriggerId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    private void validateTrigger(WorkflowTrigger workflowTrigger) throws ValidationException {
        try {
            TriggerHelper.buildTrigger(workflowTrigger);
        } catch (Exception e) {
            logger.error("Error validating workflow trigger {}", workflowTrigger, e);
            throw INVALID_WORKFLOW_TRIGGER.createException(e.getMessage());
        }
    }

    private void validateWorkflow(String workflowName, String namespace) throws ServiceException, ValidationException {
        WorkflowId workflowId = WorkflowId.build(workflowName, namespace);
        if (WorkflowService.getService().get(workflowId) == null) {
            logger.error("No workflow exists with name {} under namespace {}", workflowName, namespace);
            throw WORKFLOW_NOT_FOUND.createException(workflowName, namespace);
        }
    }

    private void validateNamespace(String name) throws ValidationException, ServiceException {
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            throw NAMESPACE_NOT_FOUND.createException(name);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow trigger service");
    }
}