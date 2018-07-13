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

import com.cognitree.kronos.model.definitions.TaskDependencyInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

import static com.cognitree.kronos.model.Task.Status.CREATED;

public class MutableTask extends MutableTaskId implements Task {

    private String id;
    private String workflowId;
    private String name;
    private String namespace;
    private String type;
    private String timeoutPolicy;
    private String maxExecutionTime;
    private List<TaskDependencyInfo> dependsOn = new ArrayList<>();
    private Map<String, Object> properties = new HashMap<>();

    private Status status = CREATED;
    private String statusMessage;
    private long createdAt;
    private long submittedAt;
    private long completedAt;

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String getTimeoutPolicy() {
        return timeoutPolicy;
    }

    public void setTimeoutPolicy(String timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
    }

    @Override
    public String getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public void setMaxExecutionTime(String maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    @Override
    public List<TaskDependencyInfo> getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(List<TaskDependencyInfo> dependsOn) {
        this.dependsOn = dependsOn;
    }

    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    @Override
    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public long getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(long submittedAt) {
        this.submittedAt = submittedAt;
    }

    @Override
    public long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(long completedAt) {
        this.completedAt = completedAt;
    }

    @JsonIgnore
    @Override
    public TaskId getIdentity() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Task)) return false;
        Task task = (Task) o;
        return Objects.equals(id, task.getId()) &&
                Objects.equals(workflowId, task.getWorkflowId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, workflowId);
    }

    @Override
    public String toString() {
        return "MutableTask{" +
                "id='" + id + '\'' +
                ", workflowId='" + workflowId + '\'' +
                ", name='" + name + '\'' +
                ", namespace='" + namespace + '\'' +
                ", type='" + type + '\'' +
                ", timeoutPolicy='" + timeoutPolicy + '\'' +
                ", maxExecutionTime='" + maxExecutionTime + '\'' +
                ", dependsOn=" + dependsOn +
                ", properties=" + properties +
                ", status=" + status +
                ", statusMessage='" + statusMessage + '\'' +
                ", createdAt=" + createdAt +
                ", submittedAt=" + submittedAt +
                ", completedAt=" + completedAt +
                '}';
    }
}
