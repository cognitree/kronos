package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class MockWorkflowDefinitionStore implements WorkflowDefinitionStore {
    @Override
    public void init(ObjectNode storeConfig) throws Exception {

    }

    @Override
    public void store(WorkflowDefinition entity) {

    }

    @Override
    public List<WorkflowDefinition> load() {
        return null;
    }

    @Override
    public WorkflowDefinition load(WorkflowDefinitionId identity) {
        return null;
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
