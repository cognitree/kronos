package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockTaskHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(MockTaskHandler.class);

    private static final List<String> tasks = Collections.synchronizedList(new ArrayList<>());

    public static boolean finishExecution(String name, String job, String namespace) {
        return tasks.add(getTaskId(name, job, namespace));
    }

    private static String getTaskId(String name, String job, String namespace) {
        return name + job + namespace;
    }

    @Override
    public void init(ObjectNode handlerConfig) {
    }

    @Override
    public Task.TaskResult handle(Task task) {
        logger.info("Received request to handle task {}", task);

        while (!tasks.contains(getTaskId(task.getName(), task.getJob(), task.getNamespace()))) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException impossible) {
                // impossible
            }
        }
        tasks.remove(task.getName());
        return Task.TaskResult.SUCCESS;
    }
}
