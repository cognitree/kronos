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

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.TestUtil;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.model.definitions.TaskDependencyInfo;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

import static com.cognitree.kronos.model.Task.Status.*;
import static com.cognitree.kronos.model.definitions.TaskDependencyInfo.Mode.all;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class MockTaskStore implements TaskStore {

    private static final Map<String, Task> TASKS_IN_STORE = new HashMap<>();

    public static Task getTask(String id) {
        return TASKS_IN_STORE.get(id);
    }

    @Override
    public void init(ObjectNode storeConfig) {
        final long currentTimeMillis = System.currentTimeMillis();
        final long timeInMillis2HoursBack = currentTimeMillis - HOURS.toMillis(2);
        final Task mockTaskOneA = TestUtil.getTaskBuilder().setId("mockTaskOne-A").setName("mockTaskOne")
                .setGroup("mockTask").setType("test").setStatus(SUCCESSFUL).setCreatedAt(timeInMillis2HoursBack)
                .setSubmittedAt(timeInMillis2HoursBack).build();
        TASKS_IN_STORE.put("mockTaskOne-A", mockTaskOneA);
        final long timeInMillis30MinsBack = currentTimeMillis - MINUTES.toMillis(30);
        final Task mockTaskOneB = TestUtil.getTaskBuilder().setId("mockTaskOne-B").setName("mockTaskOne")
                .setGroup("mockTask").setType("test").setStatus(RUNNING).setMaxExecutionTime("15m")
                .setCreatedAt(timeInMillis30MinsBack)
                .setSubmittedAt(timeInMillis30MinsBack).build();
        TASKS_IN_STORE.put("mockTaskOne-B", mockTaskOneB);
        final Task mockTaskTwo = TestUtil.getTaskBuilder().setId("mockTaskTwo").setName("mockTaskTwo")
                .setGroup("mockTask").setType("test").setStatus(CREATED).setCreatedAt(timeInMillis30MinsBack).build();
        TASKS_IN_STORE.put("mockTaskTwo", mockTaskTwo);
        final long timeInMillis20MinsBack = currentTimeMillis - MINUTES.toMillis(20);
        final List<TaskDependencyInfo> taskDependencyInfos = Arrays.asList(
                TestUtil.prepareDependencyInfo("mockTaskOne", all, "1h"),
                TestUtil.prepareDependencyInfo("mockTaskTwo", all, "1h"));
        final Task mockTaskThree = TestUtil.getTaskBuilder().setId("mockTaskThree").setName("mockTaskThree")
                .setGroup("mockTask").setType("test").setStatus(CREATED).setDependsOn(taskDependencyInfos)
                .setCreatedAt(timeInMillis20MinsBack).build();
        TASKS_IN_STORE.put("mockTaskThree", mockTaskThree);
        final Task mockTaskFour = TestUtil.getTaskBuilder().setId("mockTaskFour").setName("mockTaskFour")
                .setGroup("mockTask").setType("test").setStatus(CREATED).setCreatedAt(timeInMillis20MinsBack).build();
        TASKS_IN_STORE.put("mockTaskFour", mockTaskFour);
    }

    @Override
    public void store(Task task) {

    }

    @Override
    public List<Task> load() {
        return null;
    }

    @Override
    public Task load(TaskId identity) {
        return null;
    }

    @Override
    public void update(Task entity) {

    }

    @Override
    public void delete(TaskId identity) {

    }

    @Override
    public List<Task> load(List<Status> statuses, String namespace) {
        return new ArrayList<>(TASKS_IN_STORE.values());
    }

    @Override
    public List<Task> loadByWorkflowId(String workflowId, String namespace) {
        return null;
    }

    @Override
    public List<Task> loadByNameAndWorkflowId(String taskName, String taskGroup, String namespace) {
        return Collections.emptyList();
    }

    @Override
    public void stop() {

    }
}
