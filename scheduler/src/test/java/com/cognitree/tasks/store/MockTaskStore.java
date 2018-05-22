package com.cognitree.tasks.store;

import com.cognitree.tasks.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class MockTaskStore implements TaskStore {
    @Override
    public void init(ObjectNode storeConfig) throws Exception {

    }

    @Override
    public void store(Task task) {

    }

    @Override
    public void update(Task task) {

    }

    @Override
    public Task load(String taskId, String taskGroup) {
        return null;
    }

    @Override
    public List<Task> load(List<Task.Status> statuses) {
        return Collections.emptyList();
    }

    @Override
    public List<Task> load(String taskName, String taskGroup, long createdBefore, long createdAfter) {
        return Collections.emptyList();
    }
    @Override
    public void stop() {

    }
}
