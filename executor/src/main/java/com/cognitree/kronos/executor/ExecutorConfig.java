package com.cognitree.kronos.executor;

import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;

import java.util.Map;
import java.util.Objects;

/**
 * defines configurations for executor.
 */
public class ExecutorConfig {
    /**
     * Map of task handler configuration, required by the framework to instantiate and start the handlers ({@link TaskHandler}
     * <p>
     * Here key is the task type the handler is supposed to handle.
     */
    private Map<String, TaskHandlerConfig> taskHandlerConfig;

    public Map<String, TaskHandlerConfig> getTaskHandlerConfig() {
        return taskHandlerConfig;
    }

    public void setTaskHandlerConfig(Map<String, TaskHandlerConfig> taskHandlerConfig) {
        this.taskHandlerConfig = taskHandlerConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutorConfig)) return false;
        ExecutorConfig that = (ExecutorConfig) o;
        return Objects.equals(taskHandlerConfig, that.taskHandlerConfig);
    }

    @Override
    public int hashCode() {

        return Objects.hash(taskHandlerConfig);
    }

    @Override
    public String toString() {
        return "ExecutorConfig{" +
                "taskHandlerConfig=" + taskHandlerConfig +
                '}';
    }
}
