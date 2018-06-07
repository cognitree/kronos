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
