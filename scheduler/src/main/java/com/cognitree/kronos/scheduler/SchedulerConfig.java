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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.NamespaceStoreConfig;
import com.cognitree.kronos.scheduler.store.TaskDefinitionStore;
import com.cognitree.kronos.scheduler.store.TaskDefinitionStoreConfig;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.cognitree.kronos.scheduler.store.TaskStoreConfig;
import com.cognitree.kronos.scheduler.store.WorkflowDefinitionStore;
import com.cognitree.kronos.scheduler.store.WorkflowDefinitionStoreConfig;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.cognitree.kronos.scheduler.store.WorkflowStoreConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * defines configurations for scheduler.
 */
public class SchedulerConfig {

    /**
     * {@link NamespaceStore} configuration, required by the scheduler to instantiate the namespace store to be
     * used for storing the {@link Namespace}.
     */
    private NamespaceStoreConfig namespaceStoreConfig;
    /**
     * {@link TaskDefinitionStore} configuration, required by the scheduler to instantiate the task definition store
     * to be used for storing the {@link TaskDefinition}.
     */
    private TaskDefinitionStoreConfig taskDefinitionStoreConfig;
    /**
     * {@link TaskStore} configuration, required by the scheduler to instantiate the task store to be used for storing
     * the {@link Task}.
     */
    private TaskStoreConfig taskStoreConfig;
    /**
     * {@link WorkflowDefinitionStore} configuration, required by the scheduler to instantiate the workflow definition
     * store to be used for storing {@link WorkflowDefinition}.
     */
    private WorkflowDefinitionStoreConfig workflowDefinitionStoreConfig;
    /**
     * {@link WorkflowStore} configuration, required by the scheduler to instantiate the workflow store to be used for storing
     * the {@link Workflow}.
     */
    private WorkflowStoreConfig workflowStoreConfig;
    /**
     * Map of policy configuration, required by the scheduler to configure timeout policies to apply in case of timeout.
     * <p>
     * Here key is the policy id which is to be used while defining policy to apply on timeout.
     */
    private Map<String, TimeoutPolicyConfig> timeoutPolicyConfig = new HashMap<>();

    /**
     * Periodically, tasks older than the specified interval and status as one of the final state
     * are purged from memory to prevent the system from going OOM.
     * <p>
     * For example:
     * <ul>
     * <li>10m - 10 minutes</li>
     * <li>1h - 1 hour</li>
     * <li>1d - 1 day</li>
     * </ul>
     * <p>
     */
    private String taskPurgeInterval = "1d";

    public NamespaceStoreConfig getNamespaceStoreConfig() {
        return namespaceStoreConfig;
    }

    public void setNamespaceStoreConfig(NamespaceStoreConfig namespaceStoreConfig) {
        this.namespaceStoreConfig = namespaceStoreConfig;
    }

    public TaskDefinitionStoreConfig getTaskDefinitionStoreConfig() {
        return taskDefinitionStoreConfig;
    }

    public void setTaskDefinitionStoreConfig(TaskDefinitionStoreConfig taskDefinitionStoreConfig) {
        this.taskDefinitionStoreConfig = taskDefinitionStoreConfig;
    }

    public TaskStoreConfig getTaskStoreConfig() {
        return taskStoreConfig;
    }

    public void setTaskStoreConfig(TaskStoreConfig taskStoreConfig) {
        this.taskStoreConfig = taskStoreConfig;
    }

    public WorkflowDefinitionStoreConfig getWorkflowDefinitionStoreConfig() {
        return workflowDefinitionStoreConfig;
    }

    public void setWorkflowDefinitionStoreConfig(WorkflowDefinitionStoreConfig workflowDefinitionStoreConfig) {
        this.workflowDefinitionStoreConfig = workflowDefinitionStoreConfig;
    }

    public WorkflowStoreConfig getWorkflowStoreConfig() {
        return workflowStoreConfig;
    }

    public void setWorkflowStoreConfig(WorkflowStoreConfig workflowStoreConfig) {
        this.workflowStoreConfig = workflowStoreConfig;
    }

    public Map<String, TimeoutPolicyConfig> getTimeoutPolicyConfig() {
        return timeoutPolicyConfig;
    }

    public void setTimeoutPolicyConfig(Map<String, TimeoutPolicyConfig> timeoutPolicyConfig) {
        this.timeoutPolicyConfig = timeoutPolicyConfig;
    }

    public String getTaskPurgeInterval() {
        return taskPurgeInterval;
    }

    public void setTaskPurgeInterval(String taskPurgeInterval) {
        this.taskPurgeInterval = taskPurgeInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SchedulerConfig)) return false;
        SchedulerConfig that = (SchedulerConfig) o;
        return Objects.equals(namespaceStoreConfig, that.namespaceStoreConfig) &&
                Objects.equals(taskDefinitionStoreConfig, that.taskDefinitionStoreConfig) &&
                Objects.equals(taskStoreConfig, that.taskStoreConfig) &&
                Objects.equals(workflowDefinitionStoreConfig, that.workflowDefinitionStoreConfig) &&
                Objects.equals(workflowStoreConfig, that.workflowStoreConfig) &&
                Objects.equals(timeoutPolicyConfig, that.timeoutPolicyConfig) &&
                Objects.equals(taskPurgeInterval, that.taskPurgeInterval);
    }

    @Override
    public int hashCode() {

        return Objects.hash(namespaceStoreConfig, taskDefinitionStoreConfig, taskStoreConfig, workflowDefinitionStoreConfig, workflowStoreConfig, timeoutPolicyConfig, taskPurgeInterval);
    }

    @Override
    public String toString() {
        return "SchedulerConfig{" +
                "namespaceStoreConfig=" + namespaceStoreConfig +
                ", taskDefinitionStoreConfig=" + taskDefinitionStoreConfig +
                ", taskStoreConfig=" + taskStoreConfig +
                ", workflowDefinitionStoreConfig=" + workflowDefinitionStoreConfig +
                ", workflowStoreConfig=" + workflowStoreConfig +
                ", timeoutPolicyConfig=" + timeoutPolicyConfig +
                ", taskPurgeInterval='" + taskPurgeInterval + '\'' +
                '}';
    }
}
