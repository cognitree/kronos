package com.cognitree.kronos.scheduler.readers;

import com.cognitree.kronos.model.TaskDefinition;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cognitree.kronos.TestUtil.createTaskDefinition;

public class MockTaskDefinitionReader implements TaskDefinitionReader {
    private static final Logger logger = LoggerFactory.getLogger(MockTaskDefinitionReader.class);
    private static final Map<String, TaskDefinition> TASK_DEFINITIONS = new HashMap<>();

    public static void updateTaskDefinition(String taskName, String schedule) {
        final TaskDefinition taskDefinition = TASK_DEFINITIONS.remove(taskName);
        final TaskDefinition updatedTaskDefinition =
                createTaskDefinition(taskDefinition.getType(), taskName, schedule);
        TASK_DEFINITIONS.put(taskName, updatedTaskDefinition);
    }

    public static void removeTaskDefinition(String taskName) {
        TASK_DEFINITIONS.remove(taskName);
    }

    public static TaskDefinition getTaskDefinition(String taskName) {
        return TASK_DEFINITIONS.get(taskName);
    }

    @Override
    public void init(ObjectNode readerConfig) {
        TASK_DEFINITIONS.put("taskOne",
                createTaskDefinition("test", "taskOne", "0 0/1 * 1/1 * ? *"));
        TASK_DEFINITIONS.put("taskTwo",
                createTaskDefinition("test", "taskTwo", "0 0/5 * 1/1 * ? *"));
    }

    @Override
    public List<TaskDefinition> load() {
        logger.info("Received request to load task definitions");
        return new ArrayList<>(TASK_DEFINITIONS.values());
    }
}
