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

import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.scheduler.TaskExecutionConfig;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.*;

/**
 * A task definition holds set of attributes some required by the framework and
 * some required by the task itself during execution
 */
public class TaskDefinition {

    private String name;
    /**
     * used to segregate task with same name and type into different group,
     * by default all tasks are part of the same group which is DEFAULT
     */
    private String group = "default";
    /**
     * type of tasks
     * <p>
     * The same type should be used by the {@link TaskHandler}
     * while registering itself as the consumer of a given type of tasks
     * </p>
     */
    private String type;
    /**
     * A task can be enabled or disable at runtime, disabled tasks won't be picked by the framework
     */
    private boolean isEnabled = true;
    /**
     * A cron string representing when the task is to be scheduled
     */
    private String schedule;
    /**
     * policy to apply in case of timeout
     */
    private String timeoutPolicy;
    /**
     * max allowed time for task to finish execution.
     * <p>
     * takes precedence over the {@link TaskExecutionConfig#maxExecutionTime}
     * </p>
     */
    private String maxExecutionTime;
    /**
     * A dependency list defining tasks, current task depends on
     */
    private List<TaskDependencyInfo> dependsOn = new ArrayList<>();
    /**
     * Additional task properties used by the task during execution
     */
    private Map<String, Object> properties = new HashMap<>();
    /**
     * Any unknown properties will be mapped as additionalProperties for the task
     */
    private Map<String, Object> additionalProperties = new HashMap<>();

    public String getId() {
        return getGroup() + ":" + getName() + ":" + getType();
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

    public boolean isEnabled() {
        return isEnabled;
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
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
        return Collections.unmodifiableList(dependsOn);
    }

    public void setDependsOn(List<TaskDependencyInfo> dependsOn) {
        this.dependsOn = dependsOn;
    }

    public Map<String, Object> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(String name, Object value) {
        additionalProperties.put(name, value);
    }

    public Task createTask() {
        Task task = new Task();
        task.setName(name);
        task.setType(type);
        task.setTimeoutPolicy(timeoutPolicy);
        task.setMaxExecutionTime(maxExecutionTime);
        task.setId(UUID.randomUUID().toString());
        task.setProperties(new HashMap<>(getProperties()));
        task.getProperties().putAll(additionalProperties);
        task.setDependsOn(getDependsOn());
        task.setGroup(group);
        task.setCreatedAt(System.currentTimeMillis());
        return task;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskDefinition)) return false;
        TaskDefinition that = (TaskDefinition) o;
        return isEnabled == that.isEnabled &&
                Objects.equals(name, that.name) &&
                Objects.equals(group, that.group) &&
                Objects.equals(type, that.type) &&
                Objects.equals(schedule, that.schedule) &&
                Objects.equals(timeoutPolicy, that.timeoutPolicy) &&
                Objects.equals(maxExecutionTime, that.maxExecutionTime) &&
                Objects.equals(dependsOn, that.dependsOn) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(additionalProperties, that.additionalProperties);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, group, type, isEnabled, schedule, timeoutPolicy, maxExecutionTime, dependsOn, properties, additionalProperties);
    }

    @Override
    public String toString() {
        return "TaskDefinition{" +
                "name='" + name + '\'' +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                ", isEnabled=" + isEnabled +
                ", schedule='" + schedule + '\'' +
                ", timeoutPolicy='" + timeoutPolicy + '\'' +
                ", maxExecutionTime='" + maxExecutionTime + '\'' +
                ", dependsOn=" + dependsOn +
                ", properties=" + properties +
                ", additionalProperties=" + additionalProperties +
                '}';
    }
}
