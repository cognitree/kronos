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

import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryWorkflowStore implements WorkflowStore {

    private final Map<WorkflowId, Workflow> workflows = new HashMap<>();

    @Override
    public void init(ObjectNode storeConfig) {

    }

    @Override
    public void store(Workflow workflow) throws StoreException {
        final WorkflowId workflowId = WorkflowId.build(workflow.getName(), workflow.getNamespace());
        if (workflows.containsKey(workflowId)) {
            throw new StoreException("already exists");
        }
        workflows.put(workflowId, workflow);
    }

    @Override
    public List<Workflow> load(String namespace) {
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
        return workflows.get(WorkflowId.build(workflowId.getName(), workflowId.getNamespace()));
    }

    @Override
    public void update(Workflow workflow) throws StoreException {
        final WorkflowId workflowId = WorkflowId.build(workflow.getName(), workflow.getNamespace());
        if (!workflows.containsKey(workflowId)) {
            throw new StoreException("does not exists");
        }
        workflows.put(workflowId, workflow);
    }

    @Override
    public void delete(WorkflowId workflowId) throws StoreException {
        if (workflows.remove(WorkflowId.build(workflowId.getName(), workflowId.getNamespace())) == null) {
            throw new StoreException("does not exists");
        }
    }

    @Override
    public void stop() {

    }
}
