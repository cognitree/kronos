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

import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface Task extends TaskId {

    String getName();

    String getType();

    String getTimeoutPolicy();

    String getMaxExecutionTime();

    List<TaskDependencyInfo> getDependsOn();

    Map<String, Object> getProperties();

    Status getStatus();

    String getStatusMessage();

    long getCreatedAt();

    long getSubmittedAt();

    long getCompletedAt();

    @JsonIgnore
    TaskId getIdentity();

    enum Status {
        CREATED, WAITING, SCHEDULED, SUBMITTED, RUNNING, SUCCESSFUL, FAILED;
    }

    class TaskUpdate {
        private String taskId;
        private String workflowId;
        private String namespace;
        private Status status;
        private String statusMessage;

        public String getTaskId() {
            return taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public String getWorkflowId() {
            return workflowId;
        }

        public void setWorkflowId(String workflowId) {
            this.workflowId = workflowId;
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
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


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TaskUpdate)) return false;
            TaskUpdate that = (TaskUpdate) o;
            return Objects.equals(taskId, that.taskId) &&
                    Objects.equals(workflowId, that.workflowId) &&
                    Objects.equals(namespace, that.namespace) &&
                    status == that.status &&
                    Objects.equals(statusMessage, that.statusMessage);
        }

        @Override
        public int hashCode() {

            return Objects.hash(taskId, workflowId, namespace, status, statusMessage);
        }

        @Override
        public String toString() {
            return "TaskUpdate{" +
                    "taskId='" + taskId + '\'' +
                    ", workflowId='" + workflowId + '\'' +
                    ", namespace='" + namespace + '\'' +
                    ", status=" + status +
                    ", statusMessage='" + statusMessage + '\'' +
                    '}';
        }
    }
}
