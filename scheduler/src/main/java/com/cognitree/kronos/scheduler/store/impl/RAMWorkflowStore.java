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

import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RAMWorkflowStore implements WorkflowStore {
    private static final Logger logger = LoggerFactory.getLogger(RAMWorkflowStore.class);

    private final Map<WorkflowId, Workflow> workflows = new HashMap<>();

    @Override
    public void init(ObjectNode storeConfig) {

    }

    @Override
    public void store(Workflow workflow) throws StoreException {
        logger.debug("Received request to store workflow {}", workflow);
        final WorkflowId workflowId = WorkflowId.build(workflow.getName(), workflow.getNamespace());
        if (workflows.containsKey(workflowId)) {
            throw new StoreException("workflow with id " + workflowId + " already exists");
        }
        workflows.put(workflowId, workflow);
    }

    @Override
    public List<Workflow> load(String namespace) {
        logger.debug("Received request to get all workflows in namespace {}", namespace);
        final ArrayList<Workflow> workflows = new ArrayList<>();
        this.workflows.values().forEach(workflow -> {
            if (workflow.getNamespace().equals(namespace)) {
                workflows.add(workflow);
            }
        });
        return workflows;
    }

    @Override
    public Workflow load(WorkflowId workflowId) {
        logger.debug("Received request to load workflow with id {}", workflowId);
        return workflows.get(WorkflowId.build(workflowId.getName(), workflowId.getNamespace()));
    }

    @Override
    public void update(Workflow workflow) throws StoreException {
        logger.debug("Received request to update workflow to {}", workflow);
        final WorkflowId workflowId = WorkflowId.build(workflow.getName(), workflow.getNamespace());
        if (!workflows.containsKey(workflowId)) {
            throw new StoreException("workflow with id " + workflowId + " does not exists");
        }
        workflows.put(workflowId, workflow);
    }

    @Override
    public void delete(WorkflowId workflowId) throws StoreException {
        logger.debug("Received request to delete workflow with id {}", workflowId);
        final WorkflowId builtWorkflowId = WorkflowId.build(workflowId.getName(), workflowId.getNamespace());
        if (workflows.remove(builtWorkflowId) == null) {
            throw new StoreException("workflow with id " + builtWorkflowId + " does not exists");
        }
    }

    @Override
    public void stop() {

    }
}
