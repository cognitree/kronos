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
import java.util.Objects;

@JsonSerialize(as = Workflow.class)
@JsonDeserialize(as = Workflow.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Workflow extends WorkflowId {

    private String description;
    private List<WorkflowTask> tasks = new ArrayList<>();

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<WorkflowTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<WorkflowTask> tasks) {
        this.tasks = tasks;
    }

    @JsonIgnore
    public WorkflowId getIdentity() {
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
        return "Workflow{" +
                "description='" + description + '\'' +
                ", tasks=" + tasks +
                "} " + super.toString();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WorkflowTask {

        private String name;
        private String type;
        private Map<String, Object> properties = new HashMap<>();
        private List<String> dependsOn = new ArrayList<>();

        private String maxExecutionTime = "1d";
        private String timeoutPolicy;

        private boolean isEnabled = true;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        public List<String> getDependsOn() {
            return dependsOn;
        }

        public void setDependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn;
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
                    Objects.equals(type, that.type) &&
                    Objects.equals(properties, that.properties) &&
                    Objects.equals(dependsOn, that.dependsOn) &&
                    Objects.equals(maxExecutionTime, that.maxExecutionTime) &&
                    Objects.equals(timeoutPolicy, that.timeoutPolicy);
        }

        @Override
        public int hashCode() {

            return Objects.hash(name, type, properties, dependsOn, maxExecutionTime, timeoutPolicy, isEnabled);
        }

        @Override
        public String toString() {
            return "WorkflowTask{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", properties=" + properties +
                    ", dependsOn=" + dependsOn +
                    ", maxExecutionTime='" + maxExecutionTime + '\'' +
                    ", timeoutPolicy='" + timeoutPolicy + '\'' +
                    ", isEnabled=" + isEnabled +
                    '}';
        }
    }
}
