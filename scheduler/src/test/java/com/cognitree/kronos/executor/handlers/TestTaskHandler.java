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

package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.executor.TaskStatusListener;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;

public class TestTaskHandler implements TaskHandler {

    private static final Map<String, Integer> receivedTasks = new HashMap<>();
    private TaskStatusListener statusListener;

    // used for validating test case
    public static boolean isExecuted(String taskId) {
        return receivedTasks.containsKey(taskId) && receivedTasks.get(taskId) == 1;
    }

    @Override
    public void init(ObjectNode handlerConfig, TaskStatusListener statusListener) {
        this.statusListener = statusListener;
    }

    @Override
    public void handle(Task task) {
        int executionCount;
        if (receivedTasks.containsKey(task.getId())) {
            executionCount = receivedTasks.get(task.getId()) + 1;
        } else {
            executionCount = 1;
        }
        receivedTasks.put(task.getId(), executionCount);
        statusListener.updateStatus(task.getId(), task.getGroup(),
                (Task.Status) task.getProperties().getOrDefault("status", SUCCESSFUL));
    }
}
