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

import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockWorkflowDefinitionStore implements WorkflowDefinitionStore {
    private static final Map<WorkflowDefinitionId, WorkflowDefinition> definitionMap = new HashMap<>();

    @Override
    public void init(ObjectNode storeConfig) {

    }

    @Override
    public void store(WorkflowDefinition entity) {
        final WorkflowDefinitionId workflowDefinitionId =
                WorkflowDefinitionId.build(entity.getName(), entity.getNamespace());
        definitionMap.put(workflowDefinitionId, entity);
    }

    @Override
    public List<WorkflowDefinition> load(String namespace) {
        return Collections.emptyList();
    }

    @Override
    public WorkflowDefinition load(WorkflowDefinitionId identity) {
        return definitionMap.get(identity);
    }

    @Override
    public void update(WorkflowDefinition entity) {

    }

    @Override
    public void delete(WorkflowDefinitionId identity) {

    }

    @Override
    public void stop() {

    }
}
