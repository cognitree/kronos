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

import com.cognitree.kronos.executor.model.TaskResult;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockFailureTaskHandler implements TaskHandler {
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
    public TaskResult handle(Task task) {
        tasks.add(getTaskId(task.getName(), task.getJob(), task.getNamespace()));
        return new TaskResult(false);
    }
}
