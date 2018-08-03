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

import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockTaskDefinitionStore implements TaskDefinitionStore {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final TypeReference<List<TaskDefinition>> TASK_DEFINITION_LIST_REF =
            new TypeReference<List<TaskDefinition>>() {
            };
    private static final Map<String, TaskDefinition> taskDefinitionMap = new HashMap<>();

    @Override
    public void init(ObjectNode storeConfig) throws Exception {
        final InputStream resourceAsStream =
                MockTaskDefinitionStore.class.getClassLoader().getResourceAsStream("task-definitions.yaml");
        List<TaskDefinition> taskDefinitions = MAPPER.readValue(resourceAsStream, TASK_DEFINITION_LIST_REF);
        taskDefinitions.forEach(taskDefinition -> taskDefinitionMap.put(taskDefinition.getName(), taskDefinition));
    }

    @Override
    public void store(TaskDefinition entity) {

    }

    @Override
    public List<TaskDefinition> load() {
        return null;
    }

    @Override
    public TaskDefinition load(TaskDefinitionId identity) {
        return taskDefinitionMap.get(identity.getName());
    }

    @Override
    public void update(TaskDefinition entity) {

    }

    @Override
    public void delete(TaskDefinitionId identity) {

    }

    @Override
    public void stop() {

    }
}
