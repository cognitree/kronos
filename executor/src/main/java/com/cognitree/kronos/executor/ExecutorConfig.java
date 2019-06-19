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

package com.cognitree.kronos.executor;

import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * defines configurations for executor.
 */
public class ExecutorConfig {
    /**
     * Map of task handler configuration, required by the Kronos to instantiate and start the handlers ({@link TaskHandler}
     * <p>
     * Here key is the task type the handler is supposed to handle.
     */
    private Map<String, TaskHandlerConfig> taskHandlerConfig = new HashMap<>();

    /**
     * time duration between successive poll to queue in millisecond, defaults to 1000ms.
     */
    private long pollIntervalInMs = TimeUnit.SECONDS.toMillis(1);

    public Map<String, TaskHandlerConfig> getTaskHandlerConfig() {
        return taskHandlerConfig;
    }

    public void setTaskHandlerConfig(Map<String, TaskHandlerConfig> taskHandlerConfig) {
        this.taskHandlerConfig = taskHandlerConfig;
    }

    public long getPollIntervalInMs() {
        return pollIntervalInMs;
    }

    public void setPollIntervalInMs(long pollIntervalInMs) {
        this.pollIntervalInMs = pollIntervalInMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutorConfig)) return false;
        ExecutorConfig that = (ExecutorConfig) o;
        return pollIntervalInMs == that.pollIntervalInMs &&
                Objects.equals(taskHandlerConfig, that.taskHandlerConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskHandlerConfig, pollIntervalInMs);
    }

    @Override
    public String toString() {
        return "ExecutorConfig{" +
                "taskHandlerConfig=" + taskHandlerConfig +
                ", pollIntervalInMs=" + pollIntervalInMs +
                '}';
    }
}
