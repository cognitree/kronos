package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class MockWorkflowStore implements WorkflowStore {

    @Override
    public List<Workflow> load(String namespace, long createdAfter, long createdBefore) {
        return null;
    }

    @Override
    public List<Workflow> loadByName(String name, String namespace, long createdAfter, long createdBefore) {
        return null;
    }

    @Override
    public void init(ObjectNode storeConfig) throws Exception {

    }

    @Override
    public void store(Workflow entity) {

    }

    @Override
    public List<Workflow> load() {
        return null;
    }

    @Override
    public Workflow load(WorkflowId identity) {
        return null;
    }

    @Override
    public void update(Workflow entity) {

    }

    @Override
    public void delete(WorkflowId identity) {

    }

    @Override
    public void stop() {

    }
}
