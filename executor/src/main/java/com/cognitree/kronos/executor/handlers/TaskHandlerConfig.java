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

package com.cognitree.kronos.executor.handlers;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * defines configuration for a {@link TaskHandler}.
 */
public class TaskHandlerConfig {

    /**
     * fully qualified class name of the {@link TaskHandler} implementation.
     */
    private String handlerClass;

    /**
     * Configuration required by the handler to instantiate itself.
     * This is passed as an arg to the {@link TaskHandler#init(ObjectNode)} method at the time of instantiation.
     */
    private ObjectNode config;

    /**
     * maximum number of tasks to be scheduled in parallel for execution at any point of time.
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
                Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handlerClass, config, maxParallelTasks);
    }

    @Override
    public String toString() {
        return "TaskHandlerConfig{" +
                "handlerClass='" + handlerClass + '\'' +
                ", config=" + config +
                ", maxParallelTasks=" + maxParallelTasks +
                '}';
    }
}
