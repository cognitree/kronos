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

package com.cognitree.tasks.executor.handlers;

import com.cognitree.tasks.executor.TaskStatusListener;
import com.cognitree.tasks.model.TaskDefinition;
import com.cognitree.tasks.scheduler.policies.FailOnTimeoutPolicy;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * defines configuration for a {@link TaskHandler}
 */
public class TaskHandlerConfig {

    /**
     * fully qualified class name of the {@link TaskHandler} implementation
     */
    private String handlerClass;

    /**
     * Configuration to be passed to handler to instantiate itself.
     * This will be passed as an arg to the {@link TaskHandler#init(ObjectNode, TaskStatusListener)}
     * method at the time of instantiation along with {@link TaskStatusListener}
     */
    private ObjectNode config;

    /**
     * default time the handler is suppose to send task status should be one of the final state SUCCESSFUL/ FAILED.
     * If a task status is not received in the configured maxExecutionTime,
     * the configured {@link TaskHandlerConfig#timeoutPolicy} is applied on the task
     * <p>
     * The parameter is also configurable at a task level by setting the property maxExecutionTime in {@link TaskDefinition}
     * </p>
     */
    private String maxExecutionTime;

    /**
     * policy to be applied on task in case of timeout. If no policy is configured, then a default policy
     * ({@link FailOnTimeoutPolicy}) will be applied on the task.
     */
    private String timeoutPolicy;

    /**
     * max number of tasks to be scheduled in parallel for execution at any point of time
     */
    private int maxParallelTasks;

    public String getHandlerClass() {
        return handlerClass;
    }

    public void setHandlerClass(String handlerClass) {
        this.handlerClass = handlerClass;
    }

    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
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

    public int getMaxParallelTasks() {
        return maxParallelTasks;
    }

    public void setMaxParallelTasks(int maxParallelTasks) {
        this.maxParallelTasks = maxParallelTasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskHandlerConfig)) return false;
        TaskHandlerConfig that = (TaskHandlerConfig) o;
        return maxParallelTasks == that.maxParallelTasks &&
                Objects.equals(handlerClass, that.handlerClass) &&
                Objects.equals(config, that.config) &&
                Objects.equals(maxExecutionTime, that.maxExecutionTime) &&
                Objects.equals(timeoutPolicy, that.timeoutPolicy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handlerClass, config, maxExecutionTime, timeoutPolicy, maxParallelTasks);
    }

    @Override
    public String toString() {
        return "TaskHandlerConfig{" +
                "handlerClass='" + handlerClass + '\'' +
                ", config=" + config +
                ", maxExecutionTime='" + maxExecutionTime + '\'' +
                ", timeoutPolicy=" + timeoutPolicy +
                ", maxParallelTasks=" + maxParallelTasks +
                '}';
    }
}
