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

import com.cognitree.kronos.model.TaskDefinition;

import java.util.Objects;

/**
 * defines default configuration per task type
 */
public class TaskExecutionConfig {

    /**
     * maximum time a handler is allowed to finish executing task before it is marked as failed due to timeout.
     * The parameter can be defined at a task level as {@link TaskDefinition#maxExecutionTime} and has a higher
     * precedence.
     * </p>
     */
    private String maxExecutionTime;
    /**
     * policy to apply on task in case of timeout.
     * <p>
     * A task is said to be timed out if the handler fails to complete task execution in the configured
     * {@link TaskExecutionConfig#maxExecutionTime}.
     * A timeout policy can be defined at a task level by setting {@link TaskDefinition#timeoutPolicy} and has a higher
     * precedence.
     * </p>
     */
    private String timeoutPolicy;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskExecutionConfig)) return false;
        TaskExecutionConfig that = (TaskExecutionConfig) o;
        return Objects.equals(maxExecutionTime, that.maxExecutionTime) &&
                Objects.equals(timeoutPolicy, that.timeoutPolicy);
    }

    @Override
    public int hashCode() {

        return Objects.hash(maxExecutionTime, timeoutPolicy);
    }

    @Override
    public String toString() {
        return "TaskExecutionConfig{" +
                "maxExecutionTime='" + maxExecutionTime + '\'' +
                ", timeoutPolicy='" + timeoutPolicy + '\'' +
                '}';
    }
}
