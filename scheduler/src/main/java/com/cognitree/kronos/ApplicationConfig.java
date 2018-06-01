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

package com.cognitree.kronos;

import com.cognitree.kronos.executor.handlers.TaskHandler;
import com.cognitree.kronos.executor.handlers.TaskHandlerConfig;
import com.cognitree.kronos.model.TaskDefinition;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.queue.producer.ProducerConfig;
import com.cognitree.kronos.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.kronos.scheduler.readers.TaskDefinitionReader;
import com.cognitree.kronos.scheduler.readers.TaskDefinitionReaderConfig;
import com.cognitree.kronos.store.TaskStore;
import com.cognitree.kronos.store.TaskStoreConfig;

import java.util.Map;
import java.util.Objects;

/**
 * holds all the configuration required by the framework to instantiate itself
 */
public class ApplicationConfig {

    /**
     * Map of reader configuration, used by the framework to instantiate and start the readers ({@link TaskDefinitionReader})
     * <p>
     * Here key is the name to use for task reader and should be unique across all readers
     */
    private Map<String, TaskDefinitionReaderConfig> readerConfig;

    /**
     * Map of handler configuration, used by the framework to instantiate and start the handlers ({@link TaskHandler}
     * <p>
     * Here key is the task type the handler is supposed to handle.
     * Framework also supports a default handler concept which will be used for all the tasks with no configured handler
     * Default handler needs to be explicitly set and should have a key as "default"
     */
    private Map<String, TaskHandlerConfig> handlerConfig;

    /**
     * Map of policy configuration, used by the framework to configure timeout policies to apply in case of timeout
     * <p>
     * Here key is the policy id which is to be used by handler ({@link TaskHandlerConfig#timeoutPolicy})
     * and task {@link TaskDefinition#timeoutPolicy} while defining policy to apply on timeout.
     */
    private Map<String, TimeoutPolicyConfig> timeoutPolicyConfig;

    /**
     * {@link Producer} configuration, used by the framework to instantiate the producer to be used for communication
     * between scheduler and executor.
     */
    private ProducerConfig producerConfig;

    /**
     * {@link Consumer} configuration, used by the framework to instantiate the consumer to be used for communication
     * between scheduler and executor.
     */
    private ConsumerConfig consumerConfig;

    /**
     * {@link TaskStore} configuration, used by the framework to instantiate the task store to be used for storing
     * the task and their state.
     */
    private TaskStoreConfig taskStoreConfig;

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

    public Map<String, TaskDefinitionReaderConfig> getReaderConfig() {
        return readerConfig;
    }

    public void setReaderConfig(Map<String, TaskDefinitionReaderConfig> readerConfig) {
        this.readerConfig = readerConfig;
    }

    public Map<String, TaskHandlerConfig> getHandlerConfig() {
        return handlerConfig;
    }

    public void setHandlerConfig(Map<String, TaskHandlerConfig> handlerConfig) {
        this.handlerConfig = handlerConfig;
    }

    public Map<String, TimeoutPolicyConfig> getTimeoutPolicyConfig() {
        return timeoutPolicyConfig;
    }

    public void setTimeoutPolicyConfig(Map<String, TimeoutPolicyConfig> timeoutPolicyConfig) {
        this.timeoutPolicyConfig = timeoutPolicyConfig;
    }

    public ProducerConfig getProducerConfig() {
        return producerConfig;
    }

    public void setProducerConfig(ProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
    }

    public ConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    public void setConsumerConfig(ConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public TaskStoreConfig getTaskStoreConfig() {
        return taskStoreConfig;
    }

    public void setTaskStoreConfig(TaskStoreConfig taskStoreConfig) {
        this.taskStoreConfig = taskStoreConfig;
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
        if (!(o instanceof ApplicationConfig)) return false;
        ApplicationConfig that = (ApplicationConfig) o;
        return Objects.equals(readerConfig, that.readerConfig) &&
                Objects.equals(handlerConfig, that.handlerConfig) &&
                Objects.equals(timeoutPolicyConfig, that.timeoutPolicyConfig) &&
                Objects.equals(producerConfig, that.producerConfig) &&
                Objects.equals(consumerConfig, that.consumerConfig) &&
                Objects.equals(taskStoreConfig, that.taskStoreConfig) &&
                Objects.equals(taskPurgeInterval, that.taskPurgeInterval);
    }

    @Override
    public int hashCode() {

        return Objects.hash(readerConfig, handlerConfig, timeoutPolicyConfig, producerConfig,
                consumerConfig, taskStoreConfig, taskPurgeInterval);
    }

    @Override
    public String toString() {
        return "ApplicationConfig{" +
                "readerConfig=" + readerConfig +
                ", handlerConfig=" + handlerConfig +
                ", timeoutPolicyConfig=" + timeoutPolicyConfig +
                ", producerConfig=" + producerConfig +
                ", consumerConfig=" + consumerConfig +
                ", taskStoreConfig='" + taskStoreConfig + '\'' +
                ", taskPurgeInterval='" + taskPurgeInterval + '\'' +
                '}';
    }
}
