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
