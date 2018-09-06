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

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;

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
    private StoreConfig namespaceStoreConfig;
    /**
     * {@link TaskStore} configuration, required by the scheduler to instantiate the task store to be used for storing
     * the {@link Task}.
     */
    private StoreConfig taskStoreConfig;
    /**
     * {@link WorkflowStore} configuration, required by the scheduler to instantiate the workflow definition
     * store to be used for storing {@link Workflow}.
     */
    private StoreConfig workflowStoreConfig;
    /**
     * {@link WorkflowTriggerStore} configuration, required by the scheduler to instantiate the workflow trigger store to be
     * used for storing the {@link WorkflowTrigger}.
     */
    private StoreConfig workflowTriggerStoreConfig;
    /**
     * {@link JobStore} configuration, required by the scheduler to instantiate the workflow store to be used for storing
     * the {@link Job}.
     */
    private StoreConfig jobStoreConfig;
    /**
     * Map of policy configuration, required by the scheduler to configure timeout policies to apply in case of timeout.
     * <p>
     * Here key is the policy id which is to be used while defining policy to apply on timeout.
     */
    private Map<String, TimeoutPolicyConfig> timeoutPolicyConfig = new HashMap<>();

    public StoreConfig getNamespaceStoreConfig() {
        return namespaceStoreConfig;
    }

    public void setNamespaceStoreConfig(StoreConfig namespaceStoreConfig) {
        this.namespaceStoreConfig = namespaceStoreConfig;
    }

    public StoreConfig getTaskStoreConfig() {
        return taskStoreConfig;
    }

    public void setTaskStoreConfig(StoreConfig taskStoreConfig) {
        this.taskStoreConfig = taskStoreConfig;
    }

    public StoreConfig getWorkflowStoreConfig() {
        return workflowStoreConfig;
    }

    public void setWorkflowStoreConfig(StoreConfig workflowStoreConfig) {
        this.workflowStoreConfig = workflowStoreConfig;
    }

    public StoreConfig getWorkflowTriggerStoreConfig() {
        return workflowTriggerStoreConfig;
    }

    public void setWorkflowTriggerStoreConfig(StoreConfig workflowTriggerStoreConfig) {
        this.workflowTriggerStoreConfig = workflowTriggerStoreConfig;
    }

    public StoreConfig getJobStoreConfig() {
        return jobStoreConfig;
    }

    public void setJobStoreConfig(StoreConfig jobStoreConfig) {
        this.jobStoreConfig = jobStoreConfig;
    }

    public Map<String, TimeoutPolicyConfig> getTimeoutPolicyConfig() {
        return timeoutPolicyConfig;
    }

    public void setTimeoutPolicyConfig(Map<String, TimeoutPolicyConfig> timeoutPolicyConfig) {
        this.timeoutPolicyConfig = timeoutPolicyConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SchedulerConfig)) return false;
        SchedulerConfig that = (SchedulerConfig) o;
        return Objects.equals(namespaceStoreConfig, that.namespaceStoreConfig) &&
                Objects.equals(taskStoreConfig, that.taskStoreConfig) &&
                Objects.equals(workflowStoreConfig, that.workflowStoreConfig) &&
                Objects.equals(workflowTriggerStoreConfig, that.workflowTriggerStoreConfig) &&
                Objects.equals(jobStoreConfig, that.jobStoreConfig) &&
                Objects.equals(timeoutPolicyConfig, that.timeoutPolicyConfig);
    }

    @Override
    public int hashCode() {

        return Objects.hash(namespaceStoreConfig, taskStoreConfig, workflowStoreConfig, workflowTriggerStoreConfig, jobStoreConfig, timeoutPolicyConfig);
    }

    @Override
    public String toString() {
        return "SchedulerConfig{" +
                "namespaceStoreConfig=" + namespaceStoreConfig +
                ", taskStoreConfig=" + taskStoreConfig +
                ", workflowStoreConfig=" + workflowStoreConfig +
                ", workflowTriggerStoreConfig=" + workflowTriggerStoreConfig +
                ", jobStoreConfig=" + jobStoreConfig +
                ", timeoutPolicyConfig=" + timeoutPolicyConfig +
                '}';
    }
}
