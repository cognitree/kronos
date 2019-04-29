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

package com.cognitree.kronos.scheduler.model;

import com.cognitree.kronos.model.Policy;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@JsonSerialize(as = Workflow.class)
@JsonDeserialize(as = Workflow.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Workflow extends WorkflowId {

    private String description;
    private List<WorkflowTask> tasks = new ArrayList<>();
    private List<String> emailOnFailure = new ArrayList<>();
    private List<String> emailOnSuccess = new ArrayList<>();
    /**
     * global properties available to each task within the workflow. The task can refer to a property defined at a workflow
     * level as ${workflow.prop_name} where prop_name is the name of the property defined at workflow level.
     * <p>
     * Note: The property can be overridden by a trigger associated with this workflow, check {@link WorkflowTrigger#getProperties()}
     * for details
     */
    private Map<String, Object> properties = new HashMap<>();

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

    public List<String> getEmailOnFailure() {
        return emailOnFailure;
    }

    public void setEmailOnFailure(List<String> emailOnFailure) {
        this.emailOnFailure = emailOnFailure;
    }

    public List<String> getEmailOnSuccess() {
        return emailOnSuccess;
    }

    public void setEmailOnSuccess(List<String> emailOnSuccess) {
        this.emailOnSuccess = emailOnSuccess;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    @BsonIgnore
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
                ", emailOnFailure=" + emailOnFailure +
                ", emailOnSuccess=" + emailOnSuccess +
                ", properties=" + properties +
                "} " + super.toString();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WorkflowTask {

        private String name;
        private String type;
        private List<String> dependsOn = new ArrayList<>();
        private Map<String, Object> properties = new HashMap<>();
        private List<Policy> policies = new ArrayList<>();

        private long maxExecutionTimeInMs = TimeUnit.DAYS.toMillis(1);
        private boolean enabled = true;

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

        public List<Policy> getPolicies() {
            return policies;
        }

        public void setPolicies(List<Policy> policies) {
            this.policies = policies;
        }

        public long getMaxExecutionTimeInMs() {
            return maxExecutionTimeInMs;
        }

        public void setMaxExecutionTimeInMs(long maxExecutionTimeInMs) {
            this.maxExecutionTimeInMs = maxExecutionTimeInMs;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WorkflowTask)) return false;
            WorkflowTask that = (WorkflowTask) o;
            return maxExecutionTimeInMs == that.maxExecutionTimeInMs &&
                    enabled == that.enabled &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(type, that.type) &&
                    Objects.equals(dependsOn, that.dependsOn) &&
                    Objects.equals(properties, that.properties) &&
                    Objects.equals(policies, that.policies);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, dependsOn, properties, policies, maxExecutionTimeInMs, enabled);
        }

        @Override
        public String toString() {
            return "WorkflowTask{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", dependsOn=" + dependsOn +
                    ", properties=" + properties +
                    ", policies=" + policies +
                    ", maxExecutionTimeInMs=" + maxExecutionTimeInMs +
                    ", enabled=" + enabled +
                    '}';
        }
    }
}
