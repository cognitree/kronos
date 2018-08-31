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

import com.cognitree.kronos.model.MutableTaskId;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryTaskStore implements TaskStore {

    private final Map<TaskId, Task> tasks = new HashMap<>();

    @Override
    public void init(ObjectNode storeConfig) {
    }

    @Override
    public void store(Task task) throws StoreException {
        final TaskId taskId = MutableTaskId.build(task.getName(), task.getJob(), task.getNamespace());
        if (tasks.containsKey(taskId)) {
            throw new StoreException("already exists");
        }
        tasks.put(taskId, task);
    }

    @Override
    public List<Task> load(String namespace) {
        List<Task> tasks = new ArrayList<>();
        this.tasks.values().forEach(task -> {
            if (task.getNamespace().equals(namespace)) {
                tasks.add(task);
            }
        });
        return tasks;
    }

    @Override
    public Task load(TaskId taskId) {
        return tasks.get(MutableTaskId.build(taskId.getName(), taskId.getJob(), taskId.getNamespace()));
    }

    @Override
    public void update(Task task) throws StoreException {
        final TaskId taskId = MutableTaskId.build(task.getName(), task.getJob(), task.getNamespace());
        if (!tasks.containsKey(taskId)) {
            throw new StoreException("already exists");
        }
        tasks.put(taskId, task);
    }

    @Override
    public void delete(TaskId taskId) throws StoreException {
        if (tasks.remove(MutableTaskId.build(taskId.getName(), taskId.getJob(), taskId.getNamespace())) == null) {
            throw new StoreException("already exists");
        }
    }

    @Override
    public List<Task> load(List<Status> statuses, String namespace) {
        List<Task> tasks = new ArrayList<>();
        this.tasks.values().forEach(task -> {
            if (task.getNamespace().equals(namespace) && statuses.contains(task.getStatus())) {
                tasks.add(task);
            }
        });
        return tasks;
    }

    @Override
    public List<Task> loadByJobId(String jobId, String namespace) {
        List<Task> tasks = new ArrayList<>();
        this.tasks.values().forEach(task -> {
            if (task.getNamespace().equals(namespace) && task.getJob().equals(jobId)) {
                tasks.add(task);
            }
        });
        return tasks;
    }

    @Override
    public void stop() {

    }
}
