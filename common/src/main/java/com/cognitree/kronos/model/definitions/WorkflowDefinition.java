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

package com.cognitree.kronos.model.definitions;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

public class WorkflowDefinition extends WorkflowDefinitionId {

    private String description;
    private List<WorkflowTask> tasks = new ArrayList<>();

    private String schedule;
    private boolean isEnabled = true;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
    }

    public List<WorkflowTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<WorkflowTask> tasks) {
        this.tasks = tasks;
    }

    @JsonIgnore
    public WorkflowDefinitionId getIdentity() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowDefinition)) return false;
        WorkflowDefinition that = (WorkflowDefinition) o;
        return isEnabled == that.isEnabled &&
                Objects.equals(name, that.name) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(description, that.description) &&
                Objects.equals(schedule, that.schedule) &&
                Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, namespace, description, schedule, isEnabled, tasks);
    }

    @Override
    public String toString() {
        return "WorkflowDefinition{" +
                "name='" + name + '\'' +
                ", namespace='" + namespace + '\'' +
                ", description='" + description + '\'' +
                ", schedule='" + schedule + '\'' +
                ", isEnabled=" + isEnabled +
                ", tasks=" + tasks +
                '}';
    }

    public static class WorkflowTask {

        private String name;
        private String taskDefinitionName;
        private Map<String, Object> properties = new HashMap<>();
        private List<TaskDependencyInfo> dependsOn = new ArrayList<>();

        private String schedule;
        private String maxExecutionTime = "1d";
        private String timeoutPolicy;

        private boolean isEnabled = true;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTaskDefinitionName() {
            return taskDefinitionName;
        }

        public void setTaskDefinitionName(String taskDefinitionName) {
            this.taskDefinitionName = taskDefinitionName;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        public List<TaskDependencyInfo> getDependsOn() {
            return dependsOn;
        }

        public void setDependsOn(List<TaskDependencyInfo> dependsOn) {
            this.dependsOn = dependsOn;
        }

        public String getSchedule() {
            return schedule;
        }

        public void setSchedule(String schedule) {
            this.schedule = schedule;
        }

        public String getMaxExecutionTime() {
            return maxExecutionTime;
        }

        public void setMaxExecutionTime(String maxExecutionTime) {
            this.maxExecutionTime = maxExecutionTime;
        }

        public String getTimeoutPolicy() {
            return timeoutPolicy;
        }

        public void setTimeoutPolicy(String timeoutPolicy) {
            this.timeoutPolicy = timeoutPolicy;
        }

        public boolean isEnabled() {
            return isEnabled;
        }

        public void setEnabled(boolean enabled) {
            isEnabled = enabled;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WorkflowTask)) return false;
            WorkflowTask that = (WorkflowTask) o;
            return isEnabled == that.isEnabled &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(taskDefinitionName, that.taskDefinitionName) &&
                    Objects.equals(properties, that.properties) &&
                    Objects.equals(dependsOn, that.dependsOn) &&
                    Objects.equals(schedule, that.schedule) &&
                    Objects.equals(maxExecutionTime, that.maxExecutionTime) &&
                    Objects.equals(timeoutPolicy, that.timeoutPolicy);
        }

        @Override
        public int hashCode() {

            return Objects.hash(name, taskDefinitionName, properties, dependsOn, schedule, maxExecutionTime, timeoutPolicy, isEnabled);
        }

        @Override
        public String toString() {
            return "WorkflowTask{" +
                    "name='" + name + '\'' +
                    ", taskDefinitionName='" + taskDefinitionName + '\'' +
                    ", properties=" + properties +
                    ", dependsOn=" + dependsOn +
                    ", schedule='" + schedule + '\'' +
                    ", maxExecutionTime='" + maxExecutionTime + '\'' +
                    ", timeoutPolicy='" + timeoutPolicy + '\'' +
                    ", isEnabled=" + isEnabled +
                    '}';
        }
    }
}
