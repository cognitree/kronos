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

package com.cognitree.kronos.scheduler.store.impl;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.TaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RAMTaskStore implements TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(RAMTaskStore.class);

    private final Map<TaskId, Task> tasks = new HashMap<>();

    @Override
    public void store(Task task) throws StoreException {
        logger.debug("Received request to store task {}", task);
        final TaskId taskId = TaskId.build(task.getName(), task.getJob(), task.getNamespace());
        if (tasks.containsKey(taskId)) {
            throw new StoreException("task with id " + taskId + " already exists");
        }
        tasks.put(taskId, task);
    }

    @Override
    public List<Task> loadByStatusIn(String namespace) {
        logger.debug("Received request to get all tasks in namespace {}", namespace);
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
        logger.debug("Received request to load task with id {}", taskId);
        return tasks.get(TaskId.build(taskId.getName(), taskId.getJob(), taskId.getNamespace()));
    }

    @Override
    public List<Task> loadByJobId(String jobId, String namespace) {
        logger.debug("Received request to get all tasks with job id {}, namespace {}", jobId, namespace);
        List<Task> tasks = new ArrayList<>();
        this.tasks.values().forEach(task -> {
            if (task.getNamespace().equals(namespace) && task.getJob().equals(jobId)) {
                tasks.add(task);
            }
        });
        return tasks;
    }

    @Override
    public List<Task> loadByStatus(List<Status> statuses, String namespace) {
        logger.debug("Received request to get all tasks with status in {}, namespace {}", statuses, namespace);
        List<Task> tasks = new ArrayList<>();
        this.tasks.values().forEach(task -> {
            if (task.getNamespace().equals(namespace) && statuses.contains(task.getStatus())) {
                tasks.add(task);
            }
        });
        return tasks;
    }

    @Override
    public void update(Task task) throws StoreException {
        logger.debug("Received request to update task to {}", task);
        final TaskId taskId = TaskId.build(task.getName(), task.getJob(), task.getNamespace());
        if (!tasks.containsKey(taskId)) {
            throw new StoreException("task with id " + taskId + " does not exists");
        }
        tasks.put(taskId, task);
    }

    @Override
    public void delete(TaskId taskId) throws StoreException {
        logger.debug("Received request to delete task with id {}", taskId);
        final TaskId builtTaskId = TaskId.build(taskId.getName(), taskId.getJob(), taskId.getNamespace());
        if (tasks.remove(builtTaskId) == null) {
            throw new StoreException("task with id " + builtTaskId + " does not exists");
        }
    }
}
