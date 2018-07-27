package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class MockTaskDefinitionStore implements TaskDefinitionStore {
    @Override
    public void init(ObjectNode storeConfig) throws Exception {

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
        return null;
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
