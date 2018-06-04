package com.cognitree.kronos.scheduler.store;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

public class TaskStoreConfig {
    /**
     * fully qualified class name of the {@link TaskStore} implementation to be used to create a task store
     */
    private String taskStoreClass;

    /**
     * Configuration to be passed to task store to instantiate itself.
     * This will be passed as an arg to the method of {@link TaskStore#init(ObjectNode)} at the time of instantiation
     */
    private ObjectNode config;

    public String getTaskStoreClass() {
        return taskStoreClass;
    }

    public void setTaskStoreClass(String taskStoreClass) {
        this.taskStoreClass = taskStoreClass;
    }

    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskStoreConfig)) return false;
        TaskStoreConfig that = (TaskStoreConfig) o;
        return Objects.equals(taskStoreClass, that.taskStoreClass) &&
                Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {

        return Objects.hash(taskStoreClass, config);
    }

    @Override
    public String toString() {
        return "TaskStoreConfig{" +
                "taskStoreClass='" + taskStoreClass + '\'' +
                ", config=" + config +
                '}';
    }
}
