package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.TaskDefinition;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.kronos.scheduler.readers.TaskDefinitionReader;
import com.cognitree.kronos.scheduler.readers.TaskDefinitionReaderConfig;
import com.cognitree.kronos.store.TaskStore;
import com.cognitree.kronos.store.TaskStoreConfig;

import java.util.Map;
import java.util.Objects;

public class SchedulerConfig {

    /**
     * Map of reader configuration, used by the framework to instantiate and start the readers ({@link TaskDefinitionReader})
     * <p>
     * Here key is the name to use for task reader and should be unique across all readers
     */
    private Map<String, TaskDefinitionReaderConfig> taskReaderConfig;

    /**
     * {@link TaskStore} configuration, used by the framework to instantiate the task store to be used for storing
     * the task and their state.
     */
    private TaskStoreConfig taskStoreConfig;

    private Map<String, TaskExecutionConfig> taskExecutionConfig;

    /**
     * Map of policy configuration, used by the framework to configure timeout policies to apply in case of timeout
     * <p>
     * Here key is the policy id which is to be used by handler ({@link TaskExecutionConfig#timeoutPolicy})
     * and task {@link TaskDefinition#timeoutPolicy} while defining policy to apply on timeout.
     */
    private Map<String, TimeoutPolicyConfig> timeoutPolicyConfig;

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

    public Map<String, TaskDefinitionReaderConfig> getTaskReaderConfig() {
        return taskReaderConfig;
    }

    public void setTaskReaderConfig(Map<String, TaskDefinitionReaderConfig> taskReaderConfig) {
        this.taskReaderConfig = taskReaderConfig;
    }

    public TaskStoreConfig getTaskStoreConfig() {
        return taskStoreConfig;
    }

    public void setTaskStoreConfig(TaskStoreConfig taskStoreConfig) {
        this.taskStoreConfig = taskStoreConfig;
    }

    public Map<String, TaskExecutionConfig> getTaskExecutionConfig() {
        return taskExecutionConfig;
    }

    public void setTaskExecutionConfig(Map<String, TaskExecutionConfig> taskExecutionConfig) {
        this.taskExecutionConfig = taskExecutionConfig;
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
        return Objects.equals(taskReaderConfig, that.taskReaderConfig) &&
                Objects.equals(taskStoreConfig, that.taskStoreConfig) &&
                Objects.equals(taskExecutionConfig, that.taskExecutionConfig) &&
                Objects.equals(timeoutPolicyConfig, that.timeoutPolicyConfig) &&
                Objects.equals(taskPurgeInterval, that.taskPurgeInterval);
    }

    @Override
    public int hashCode() {

        return Objects.hash(taskReaderConfig, taskStoreConfig, taskExecutionConfig, timeoutPolicyConfig, taskPurgeInterval);
    }

    @Override
    public String toString() {
        return "SchedulerConfig{" +
                "taskReaderConfig=" + taskReaderConfig +
                ", taskStoreConfig=" + taskStoreConfig +
                ", taskExecutionConfig=" + taskExecutionConfig +
                ", timeoutPolicyConfig=" + timeoutPolicyConfig +
                ", taskPurgeInterval='" + taskPurgeInterval + '\'' +
                '}';
    }
}
