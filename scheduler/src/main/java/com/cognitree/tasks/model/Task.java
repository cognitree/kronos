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

package com.cognitree.tasks.model;

import com.cognitree.tasks.executor.TaskStatusListener;
import com.cognitree.tasks.executor.handlers.TaskHandler;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

import static com.cognitree.tasks.model.Task.Status.CREATED;

/**
 * A task created from {@link TaskDefinition} for execution
 */
public class Task {

    /**
     * is set by the framework uniquely to identify this task
     */
    private String id;
    /**
     * name of the task same as {@link TaskDefinition#name}
     */
    private String name;
    /**
     * group name task belongs to same as {@link TaskDefinition#group}
     */
    private String group;
    /**
     * type of task same as {@link TaskDefinition#type}
     */
    private String type;
    /**
     * policy to apply in case of timeout
     */
    private String timeoutPolicy;
    /**
     * max allowed time for task to finish execution same as {@link TaskDefinition#maxExecutionTime}.
     * <p>
     * takes precedence over the {@link com.cognitree.tasks.executor.handlers.TaskHandlerConfig#maxExecutionTime}
     * </p>
     */
    private String maxExecutionTime;
    /**
     * list of tasks it depends on defined same as {@link TaskDefinition#dependsOn}
     */
    private List<TaskDependencyInfo> dependsOn = new ArrayList<>();
    /**
     * properties used by the task during execution same as {@link TaskDefinition#properties}
     * It also includes all the additional task properties {@link TaskDefinition#additionalProperties}
     */
    private Map<String, Object> properties = new HashMap<>();
    /**
     * runtime properties/ metadata of the task set while notifying the task status
     * {@link TaskStatusListener#updateStatus(String, String, Status, String, ObjectNode)}
     */
    private ObjectNode runtimeProperties;
    private Status status = CREATED;
    /**
     * additional details about task status
     */
    private String statusMessage;
    /**
     * creation time of the task as per the {@link TaskDefinition#schedule}
     */
    private long createdAt;
    /**
     * actual time when the task is submitted for execution to the {@link TaskHandler}
     */
    private long submittedAt;
    /**
     * actual time when the task is submitted for execution to the {@link TaskHandler}
     */
    private long completedAt;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTimeoutPolicy() {
        return timeoutPolicy;
    }

    public void setTimeoutPolicy(String timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
    }

    public String getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public void setMaxExecutionTime(String maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    public List<TaskDependencyInfo> getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(List<TaskDependencyInfo> dependsOn) {
        this.dependsOn = dependsOn;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public ObjectNode getRuntimeProperties() {
        return runtimeProperties;
    }

    public void setRuntimeProperties(ObjectNode runtimeProperties) {
        this.runtimeProperties = runtimeProperties;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(long submittedAt) {
        this.submittedAt = submittedAt;
    }

    public long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(long completedAt) {
        this.completedAt = completedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Task)) return false;
        Task task = (Task) o;
        return Objects.equals(id, task.id) &&
                Objects.equals(name, task.name) &&
                Objects.equals(group, task.group) &&
                Objects.equals(type, task.type) &&
                Objects.equals(dependsOn, task.dependsOn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, group, type, dependsOn);
    }

    @Override
    public String toString() {
        return "Task{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                ", timeoutPolicy='" + timeoutPolicy + '\'' +
                ", maxExecutionTime='" + maxExecutionTime + '\'' +
                ", dependsOn=" + dependsOn +
                ", properties=" + properties +
                ", runtimeProperties=" + runtimeProperties +
                ", status=" + status +
                ", statusMessage='" + statusMessage + '\'' +
                ", createdAt=" + createdAt +
                ", submittedAt=" + submittedAt +
                ", completedAt=" + completedAt +
                '}';
    }

    public enum Status {
        CREATED, WAITING, SUBMITTED, RUNNING, SUCCESSFUL, FAILED;
    }
}
