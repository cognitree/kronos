package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TypeATaskHandler implements TaskHandler {
    private static final Set<String> handledTasks = Collections.synchronizedSet(new HashSet<>());

    public static boolean isHandled(String taskId) {
        return handledTasks.contains(taskId);
    }

    @Override
    public void init(ObjectNode handlerConfig) {

    }

    @Override
    public void handle(Task task) throws HandlerException {
        handledTasks.add(task.getId());
    }
}