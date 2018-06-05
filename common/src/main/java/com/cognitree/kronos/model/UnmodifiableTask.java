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

package com.cognitree.kronos.model;

import java.util.List;
import java.util.Map;

/**
 * A task created from {@link TaskDefinition} for execution.
 */
public class UnmodifiableTask extends Task {
    private final Task task;

    public UnmodifiableTask(Task task) {
        this.task = task;
    }

    public String getId() {
        return task.getId();
    }

    public void setId(String id) {
        throw new UnsupportedOperationException();
    }

    public String getName() {
        return task.getName();
    }

    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    public String getGroup() {
        return task.getGroup();
    }

    public void setGroup(String group) {
        throw new UnsupportedOperationException();
    }

    public String getType() {
        return task.getType();
    }

    public void setType(String type) {
        throw new UnsupportedOperationException();
    }

    public String getTimeoutPolicy() {
        return task.getTimeoutPolicy();
    }

    public void setTimeoutPolicy(String timeoutPolicy) {
        throw new UnsupportedOperationException();
    }

    public String getMaxExecutionTime() {
        return task.getMaxExecutionTime();
    }

    public void setMaxExecutionTime(String maxExecutionTime) {
        throw new UnsupportedOperationException();
    }

    public List<TaskDependencyInfo> getDependsOn() {
        return task.getDependsOn();
    }

    public void setDependsOn(List<TaskDependencyInfo> dependsOn) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Object> getProperties() {
        return task.getProperties();
    }

    public void setProperties(Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }

    public Status getStatus() {
        return task.getStatus();
    }

    public void setStatus(Status status) {
        throw new UnsupportedOperationException();
    }

    public String getStatusMessage() {
        return task.getStatusMessage();
    }

    public void setStatusMessage(String statusMessage) {
        throw new UnsupportedOperationException();
    }

    public long getCreatedAt() {
        return task.getCreatedAt();
    }

    public void setCreatedAt(long createdAt) {
        throw new UnsupportedOperationException();
    }

    public long getSubmittedAt() {
        return task.getSubmittedAt();
    }

    public void setSubmittedAt(long submittedAt) {
        throw new UnsupportedOperationException();
    }

    public long getCompletedAt() {
        return task.getCompletedAt();
    }

    public void setCompletedAt(long completedAt) {
        throw new UnsupportedOperationException();
    }
}
