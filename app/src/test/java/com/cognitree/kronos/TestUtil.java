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

package com.cognitree.kronos;

import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDependencyInfo;

import java.util.*;

import static com.cognitree.kronos.model.Task.Status.CREATED;

public class TestUtil {

    public static TaskDependencyInfo prepareDependencyInfo(String taskName, TaskDependencyInfo.Mode mode, String duration) {
        TaskDependencyInfo dependencyInfo1 = new TaskDependencyInfo();
        dependencyInfo1.setName(taskName);
//        dependencyInfo1.setDuration(duration);
        dependencyInfo1.setMode(mode);
        return dependencyInfo1;
    }

    public static TaskDefinition createTaskDefinition(String type, String name, String schedule) {
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setType(type);
        taskDefinition.setName(name);
        return taskDefinition;
    }

    public static void sleep(long timeInMillis) {
        try {
            Thread.sleep(timeInMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static MockTaskBuilder getTaskBuilder() {
        return new MockTaskBuilder();
    }

    public static class MockTaskBuilder {
        private String id = UUID.randomUUID().toString();
        private String name;
        private String group = "default";
        private String type;
        private String timeoutPolicy;
        private String maxExecutionTime = "1h";
        private List<TaskDependencyInfo> dependsOn = new ArrayList<>();
        private Map<String, Object> properties = new HashMap<>();
        private Status status = CREATED;
        private String statusMessage;
        private long createdAt = System.currentTimeMillis();
        private long submittedAt;
        private long completedAt;

        {
            this.properties.put("shouldPass", true);
            this.properties.put("waitForCallback", false);
        }

        public MockTaskBuilder setId(String id) {
            this.id = id;
            return this;
        }


        public MockTaskBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public MockTaskBuilder setGroup(String group) {
            this.group = group;
            return this;
        }

        public MockTaskBuilder setType(String type) {
            this.type = type;
            return this;
        }

        public MockTaskBuilder setTimeoutPolicy(String timeoutPolicy) {
            this.timeoutPolicy = timeoutPolicy;
            return this;
        }

        public MockTaskBuilder setMaxExecutionTime(String maxExecutionTime) {
            this.maxExecutionTime = maxExecutionTime;
            return this;
        }

        public MockTaskBuilder setDependsOn(List<TaskDependencyInfo> dependsOn) {
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

        public MockTaskBuilder shouldPass(boolean shouldPass) {
            this.properties.put("shouldPass", shouldPass);
            return this;
        }

        public MockTaskBuilder waitForCallback(boolean waitForCallback) {
            this.properties.put("waitForCallback", waitForCallback);
            return this;
        }

        public Task build() {
            MutableTask task = new MutableTask();
            task.setId(id);
            task.setName(name);
            task.setType(type);
            task.setStatus(status);
            task.setStatusMessage(statusMessage);
            task.setTimeoutPolicy(timeoutPolicy);
            task.setMaxExecutionTime(maxExecutionTime);
            task.setProperties(properties);
            task.setDependsOn(dependsOn);
            task.setWorkflowId(group);
            task.setCreatedAt(createdAt);
            task.setSubmittedAt(submittedAt);
            task.setCompletedAt(completedAt);
            return task;
        }
    }

}
