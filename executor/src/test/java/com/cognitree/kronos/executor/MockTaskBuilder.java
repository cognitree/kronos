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

package com.cognitree.kronos.executor;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.cognitree.kronos.model.Task.Status.CREATED;

public class MockTaskBuilder {
    private String name = UUID.randomUUID().toString();
    private String namespace;
    private String job;
    private String type;
    private long maxExecutionTime = TimeUnit.HOURS.toMillis(1);
    private List<String> dependsOn = new ArrayList<>();
    private Map<String, Object> properties = new HashMap<>();
    private Status status = CREATED;
    private String statusMessage;
    private long createdAt = System.currentTimeMillis();
    private long submittedAt;
    private long completedAt;

    public static MockTaskBuilder getTaskBuilder() {
        return new MockTaskBuilder();
    }

    public MockTaskBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public MockTaskBuilder setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public MockTaskBuilder setJob(String job) {
        this.job = job;
        return this;
    }

    public MockTaskBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public MockTaskBuilder setMaxExecutionTime(long maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
        return this;
    }

    public MockTaskBuilder setDependsOn(List<String> dependsOn) {
        this.dependsOn = dependsOn;
        return this;
    }

    public MockTaskBuilder setStatus(Status status) {
        this.status = status;
        return this;
    }

    public MockTaskBuilder setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
        return this;
    }

    public MockTaskBuilder setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    public MockTaskBuilder setSubmittedAt(long submittedAt) {
        this.submittedAt = submittedAt;
        return this;
    }

    public MockTaskBuilder setCompletedAt(long completedAt) {
        this.completedAt = completedAt;
        return this;
    }

    public Task build() {
        Task task = new Task();
        task.setName(name);
        task.setNamespace(namespace);
        task.setJob(job);
        task.setType(type);
        task.setStatus(status);
        task.setStatusMessage(statusMessage);
        task.setMaxExecutionTimeInMs(maxExecutionTime);
        task.setProperties(properties);
        task.setDependsOn(dependsOn);
        task.setCreatedAt(createdAt);
        task.setSubmittedAt(submittedAt);
        task.setCompletedAt(completedAt);
        return task;
    }
}
