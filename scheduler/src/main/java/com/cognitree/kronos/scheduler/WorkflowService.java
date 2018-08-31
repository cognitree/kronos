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
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

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

    public void add(Workflow workflow) throws ValidationException, ServiceException {
        logger.debug("Received request to add workflow {}", workflow);
        validateNamespace(workflow.getNamespace());
        validate(workflow);
        try {
            workflowStore.store(workflow);
        } catch (StoreException e) {
            logger.error("unable to store workflow {}", workflow, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Workflow> get(String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all workflow under namespace {}", namespace);
        validateNamespace(namespace);
        try {
            return workflowStore.load(namespace);
        } catch (StoreException e) {
            logger.error("unable to get all workflow under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public Workflow get(WorkflowId workflowId) throws ServiceException, ValidationException {
        logger.debug("Received request to get workflow {}", workflowId);
        validateNamespace(workflowId.getNamespace());
        try {
            return workflowStore.load(workflowId);
        } catch (StoreException e) {
            logger.error("unable to get workflow {}", workflowId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void update(Workflow workflow) throws ValidationException, SchedulerException, ServiceException {
        logger.debug("Received request to update workflow {}", workflow);
        validateNamespace(workflow.getNamespace());
        validate(workflow);
        try {
            workflowStore.update(workflow);
            WorkflowSchedulerService.getService().update(workflow);
        } catch (StoreException e) {
            logger.error("unable to update workflow {}", workflow, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void delete(WorkflowId workflowId) throws SchedulerException, ServiceException, ValidationException {
        logger.debug("Received request to delete workflow {}", workflowId);
        validateNamespace(workflowId.getNamespace());
        try {
            // delete all triggers before deleting workflow
            final List<WorkflowTrigger> workflowTriggers =
                    WorkflowTriggerService.getService().get(workflowId.getName(), workflowId.getNamespace());
            for (WorkflowTrigger workflowTrigger : workflowTriggers) {
                WorkflowTriggerService.getService().delete(workflowTrigger);
            }
            workflowStore.delete(workflowId);
        } catch (StoreException e) {
            logger.error("unable to delete workflow {}", workflowId, e);
            throw new ServiceException(e.getMessage());
        }

    }

    private void validateNamespace(String name) throws ValidationException, ServiceException {
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            throw new ValidationException("no namespace exists with name " + name);
        }
    }

    /**
     * validate workflow
     *
     * @param workflow
     * @return
     */
    private void validate(Workflow workflow) throws ValidationException {
        final HashMap<String, Workflow.WorkflowTask> workflowTaskMap = new HashMap<>();
        final TopologicalSort<Workflow.WorkflowTask> topologicalSort = new TopologicalSort<>();
        final List<Workflow.WorkflowTask> workflowTasks = workflow.getTasks();
        for (Workflow.WorkflowTask task : workflowTasks) {
            if (task.isEnabled()) {
                workflowTaskMap.put(task.getName(), task);
                topologicalSort.add(task);
            }
        }

        for (Workflow.WorkflowTask workflowTask : workflowTasks) {
            final List<String> dependsOn = workflowTask.getDependsOn();
            if (dependsOn != null && !dependsOn.isEmpty()) {
                for (String dependentTask : dependsOn) {
                    final Workflow.WorkflowTask dependeeTask = workflowTaskMap.get(dependentTask);
                    if (dependeeTask == null) {
                        throw new ValidationException("missing task " + dependentTask);
                    }
                    topologicalSort.add(dependeeTask, workflowTask);
                }
            }
        }
        if (!topologicalSort.isDag()) {
            throw new ValidationException("Invalid workflow contains cyclic dependency)");
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow service");
        workflowStore.stop();
    }
}