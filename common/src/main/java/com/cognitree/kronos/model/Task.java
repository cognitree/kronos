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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cognitree.kronos.model.Task.Status.CREATED;

@JsonSerialize(as = Task.class)
@JsonDeserialize(as = Task.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Task extends TaskId {

    private String type;
    private String timeoutPolicy;
    private String maxExecutionTime;
    private List<String> dependsOn = new ArrayList<>();
    private Map<String, Object> properties = new HashMap<>();
    private Map<String, Object> context = new HashMap<>();

    private Status status = CREATED;
    private String statusMessage;
    private Long createdAt;
    private Long submittedAt;
    private Long completedAt;

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

    public List<String> getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(List<String> dependsOn) {
        this.dependsOn = dependsOn;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public void setContext(Map<String, Object> context) {
        this.context = context;
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

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(Long submittedAt) {
        this.submittedAt = submittedAt;
    }

    public Long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Long completedAt) {
        this.completedAt = completedAt;
    }

    @JsonIgnore
    public TaskId getIdentity() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "Task{" +
                "type='" + type + '\'' +
                ", timeoutPolicy='" + timeoutPolicy + '\'' +
                ", maxExecutionTime='" + maxExecutionTime + '\'' +
                ", dependsOn=" + dependsOn +
                ", properties=" + properties +
                ", context=" + context +
                ", status=" + status +
                ", statusMessage='" + statusMessage + '\'' +
                ", createdAt=" + createdAt +
                ", submittedAt=" + submittedAt +
                ", completedAt=" + completedAt +
                "} " + super.toString();
    }

    public enum Status {
        CREATED(false),
        WAITING(false),
        SCHEDULED(false),
        SUBMITTED(false),
        RUNNING(false),
        SUCCESSFUL(true),
        FAILED(true);

        private final boolean isFinal;

        Status(boolean isFinal) {
            this.isFinal = isFinal;
        }

        public boolean isFinal() {
            return this.isFinal;
        }
    }
}
