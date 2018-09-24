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

package com.cognitree.kronos.scheduler.store.impl;

import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RAMWorkflowTriggerStore implements WorkflowTriggerStore {
    private static final Logger logger = LoggerFactory.getLogger(RAMWorkflowTriggerStore.class);
    private final Map<WorkflowTriggerId, WorkflowTrigger> workflowTriggers = new HashMap<>();

    @Override
    public void store(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to store workflow trigger {}", workflowTrigger);
        final WorkflowTriggerId triggerId = WorkflowTriggerId.build(workflowTrigger.getNamespace(), workflowTrigger.getName(),
                workflowTrigger.getWorkflow());
        if (workflowTriggers.containsKey(triggerId)) {
            throw new StoreException("workflow trigger with id " + triggerId + " already exists");
        }
        workflowTriggers.put(triggerId, workflowTrigger);
    }

    @Override
    public List<WorkflowTrigger> load(String namespace) {
        logger.debug("Received request to get all workflow triggers under namespace {}", namespace);
        final ArrayList<WorkflowTrigger> workflowTriggers = new ArrayList<>();
        this.workflowTriggers.values().forEach(workflowTrigger -> {
            if (workflowTrigger.getNamespace().equals(namespace)) {
                workflowTriggers.add(workflowTrigger);
            }
        });
        return workflowTriggers;
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowName(String namespace, String workflowName) {
        logger.debug("Received request to get all workflow triggers with workflow name {} under namespace {}",
                workflowName, namespace);
        final ArrayList<WorkflowTrigger> workflowTriggers = new ArrayList<>();
        this.workflowTriggers.values().forEach(workflowTrigger -> {
            if (workflowTrigger.getWorkflow().equals(workflowName) && workflowTrigger.getNamespace().equals(namespace)) {
                workflowTriggers.add(workflowTrigger);
            }
        });
        return workflowTriggers;
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowNameAndEnabled(String namespace, String workflowName, boolean enabled) throws StoreException {
        logger.debug("Received request to get all enabled {} workflow triggers with workflow name {} under namespace {}",
                enabled, workflowName, namespace);
        final ArrayList<WorkflowTrigger> workflowTriggers = new ArrayList<>();
        this.workflowTriggers.values().forEach(workflowTrigger -> {
            if (workflowTrigger.getWorkflow().equals(workflowName) && workflowTrigger.getNamespace().equals(namespace)
                    && workflowTrigger.isEnabled() == enabled) {
                workflowTriggers.add(workflowTrigger);
            }
        });
        return workflowTriggers;
    }

    @Override
    public WorkflowTrigger load(WorkflowTriggerId triggerId) {
        logger.debug("Received request to load workflow trigger with id {}", triggerId);
        return workflowTriggers.get(WorkflowTriggerId.build(triggerId.getNamespace(), triggerId.getName(),
                triggerId.getWorkflow()));
    }

    @Override
    public void update(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to update workflow trigger to {}", workflowTrigger);
        final WorkflowTriggerId triggerId = WorkflowTriggerId.build(workflowTrigger.getNamespace(), workflowTrigger.getName(),
                workflowTrigger.getWorkflow());
        if (!workflowTriggers.containsKey(triggerId)) {
            throw new StoreException("workflow trigger with id " + triggerId + " does not exists");
        }
        workflowTriggers.put(triggerId, workflowTrigger);
    }

    @Override
    public void delete(WorkflowTriggerId triggerId) throws StoreException {
        logger.debug("Received request to delete workflow trigger with id {}", triggerId);
        final WorkflowTriggerId builtTriggerId = WorkflowTriggerId.build(triggerId.getNamespace(), triggerId.getName(),
                triggerId.getWorkflow());
        if (workflowTriggers.remove(builtTriggerId) == null) {
            throw new StoreException("workflow trigger with id " + builtTriggerId + " does not exists");
        }
    }
}
