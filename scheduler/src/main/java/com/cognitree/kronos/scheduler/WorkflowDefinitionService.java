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
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.WorkflowDefinitionStore;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class WorkflowDefinitionService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowDefinitionService.class);

    private final StoreConfig storeConfig;
    private WorkflowDefinitionStore workflowDefinitionStore;

    public WorkflowDefinitionService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static WorkflowDefinitionService getService() {
        return (WorkflowDefinitionService) ServiceProvider.getService(WorkflowDefinitionService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        workflowDefinitionStore = (WorkflowDefinitionStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowDefinitionStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {

    }

    public void add(WorkflowDefinition workflowDefinition) throws ValidationException, SchedulerException {
        logger.debug("Received request to add workflow definition {}", workflowDefinition);
        validate(workflowDefinition);
        if (workflowDefinition.getSchedule() != null) {
            WorkflowSchedulerService.getService().schedule(workflowDefinition);
        }
        workflowDefinitionStore.store(workflowDefinition);
    }

    public List<WorkflowDefinition> get(String namespace) {
        return workflowDefinitionStore.load(namespace);
    }

    public WorkflowDefinition get(WorkflowDefinitionId workflowDefinitionId) {
        return workflowDefinitionStore.load(workflowDefinitionId);
    }

    public void update(WorkflowDefinition workflowDefinition) throws ValidationException, SchedulerException {
        logger.debug("Received request to update workflow definition {}", workflowDefinition);
        validate(workflowDefinition);
        // delete the old workflow definition
        WorkflowSchedulerService.getService().delete(workflowDefinition.getIdentity());
        // schedule new workflow only if schedule is present
        if (workflowDefinition.getSchedule() != null) {
            WorkflowSchedulerService.getService().schedule(workflowDefinition);
        }
        workflowDefinitionStore.update(workflowDefinition);
    }

    public Workflow execute(WorkflowDefinition workflowDefinition) {
        logger.debug("Received request to execute workflow definition {}", workflowDefinition);
        return WorkflowSchedulerService.getService().execute(workflowDefinition.getName(),
                workflowDefinition.getNamespace(), workflowDefinition.getTasks());
    }

    public void delete(WorkflowDefinitionId workflowDefinitionId) throws SchedulerException {
        logger.debug("Received request to delete workflow definition {}", workflowDefinitionId);
        WorkflowSchedulerService.getService().delete(workflowDefinitionId);
        workflowDefinitionStore.delete(workflowDefinitionId);
    }

    /**
     * validate workflow definition
     *
     * @param workflowDefinition
     * @return
     */
    public void validate(WorkflowDefinition workflowDefinition) throws ValidationException {
        final HashMap<String, WorkflowDefinition.WorkflowTask> workflowTaskMap = new HashMap<>();
        final TopologicalSort<WorkflowDefinition.WorkflowTask> topologicalSort = new TopologicalSort<>();
        final List<WorkflowDefinition.WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        for (WorkflowDefinition.WorkflowTask task : workflowTasks) {
            final String taskDefinitionName = task.getTaskDefinitionName();
            if (TaskDefinitionService.getService().get(TaskDefinitionId.build(taskDefinitionName)) == null) {
                throw new ValidationException("missing task definition with name " + taskDefinitionName);
            }

            final String taskName = task.getName();
            if (task.isEnabled()) {
                workflowTaskMap.put(taskName, task);
                topologicalSort.add(task);
            }
        }

        for (WorkflowDefinition.WorkflowTask workflowTask : workflowTasks) {
            final List<String> dependsOn = workflowTask.getDependsOn();
            if (dependsOn != null && !dependsOn.isEmpty()) {
                for (String dependentTask : dependsOn) {
                    final WorkflowDefinition.WorkflowTask dependeeTask = workflowTaskMap.get(dependentTask);
                    if (dependeeTask == null) {
                        throw new ValidationException("missing task " + dependentTask);
                    }
                    topologicalSort.add(dependeeTask, workflowTask);
                }
            }
        }
        if (!topologicalSort.isDag()) {
            throw new ValidationException("Invalid workflow definition contains cyclic dependency)");
        }
    }

    @Override
    public void stop() {
        workflowDefinitionStore.stop();
    }
}