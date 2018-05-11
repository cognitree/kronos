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

import com.cognitree.tasks.executor.TaskExecutionService;
import com.cognitree.tasks.model.Task.Status;
import com.cognitree.tasks.scheduler.TaskProviderService;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * A helper POJO used by {@link TaskProviderService} and {@link TaskExecutionService} to push task status update to queue
 */
public class TaskStatus {
    private String taskId;
    private String taskGroup;
    private Status status;
    private String statusMessage;
    private ObjectNode runtimeProperties;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskGroup() {
        return taskGroup;
    }

    public void setTaskGroup(String taskGroup) {
        this.taskGroup = taskGroup;
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

    public ObjectNode getRuntimeProperties() {
        return runtimeProperties;
    }

    public void setRuntimeProperties(ObjectNode runtimeProperties) {
        this.runtimeProperties = runtimeProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskStatus)) return false;
        TaskStatus that = (TaskStatus) o;
        return Objects.equals(taskId, that.taskId) &&
                Objects.equals(taskGroup, that.taskGroup) &&
                status == that.status &&
                Objects.equals(statusMessage, that.statusMessage) &&
                Objects.equals(runtimeProperties, that.runtimeProperties);
    }

    @Override
    public int hashCode() {

        return Objects.hash(taskId, taskGroup, status, statusMessage, runtimeProperties);
    }

    @Override
    public String toString() {
        return "TaskStatus{" +
                "taskId='" + taskId + '\'' +
                ", taskGroup='" + taskGroup + '\'' +
                ", status=" + status +
                ", statusMessage='" + statusMessage + '\'' +
                ", runtimeProperties=" + runtimeProperties +
                '}';
    }
}
