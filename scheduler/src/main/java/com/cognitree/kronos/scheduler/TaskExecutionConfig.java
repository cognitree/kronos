package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.TaskDefinition;

import java.util.Objects;

public class TaskExecutionConfig {

    /**
     * max allowed time for the handler to finish executing task.
     * The parameter can be defined at a task level by setting {@link TaskDefinition#maxExecutionTime} and has a higher
     * precedence.
     * </p>
     */
    protected String maxExecutionTime;
    /**
     * policy to apply on task in case of timeout.
     * <p>
     * A task is said to be timed out if the handler fails to complete task execution in configured
     * {@link TaskExecutionConfig#maxExecutionTime}.
     * A timeout policy can be defined at a task level by setting {@link TaskDefinition#timeoutPolicy} and has a higher
     * precedence.
     * </p>
     */
    protected String timeoutPolicy;

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
