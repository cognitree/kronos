package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockSuccessTaskHandler implements TaskHandler {
    private static final List<String> tasks = Collections.synchronizedList(new ArrayList<>());

    public static boolean isHandled(String name, String job, String namespace) {
        return tasks.contains(getTaskId(name, job, namespace));
    }

    private static String getTaskId(String name, String job, String namespace) {
        return name + job + namespace;
    }

    @Override
    public void init(ObjectNode handlerConfig) {

    }

    @Override
    public Task.TaskResult handle(Task task) {
        tasks.add(getTaskId(task.getName(), task.getJob(), task.getNamespace()));
        return Task.TaskResult.SUCCESS;
    }
}
