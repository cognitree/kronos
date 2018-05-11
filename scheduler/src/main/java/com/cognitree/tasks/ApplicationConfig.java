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

package com.cognitree.tasks;

import com.cognitree.tasks.executor.handlers.TaskHandler;
import com.cognitree.tasks.executor.handlers.TaskHandlerConfig;
import com.cognitree.tasks.model.TaskDefinition;
import com.cognitree.tasks.queue.consumer.Consumer;
import com.cognitree.tasks.queue.consumer.ConsumerConfig;
import com.cognitree.tasks.queue.producer.Producer;
import com.cognitree.tasks.queue.producer.ProducerConfig;
import com.cognitree.tasks.scheduler.policies.TimeoutPolicyConfig;
import com.cognitree.tasks.scheduler.readers.TaskDefinitionReader;
import com.cognitree.tasks.scheduler.readers.TaskDefinitionReaderConfig;

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
     * {@link Producer} configuration, used by the framework to instantiate the task producer to be used for adding
     * tasks to queue.
     */
    private ProducerConfig taskProducerConfig;
    /**
     * {@link Consumer} configuration, used by the framework to instantiate the task consumer to be used for getting
     * tasks from queue.
     */
    private ConsumerConfig taskConsumerConfig;

    /**
     * {@link Producer} configuration, used by the framework to instantiate the task status producer to be used for adding
     * tasks status to queue.
     */
    private ProducerConfig taskStatusProducerConfig;
    /**
     * {@link Consumer} configuration, used by the framework to instantiate the task status consumer to be used for getting
     * tasks status from queue.
     */
    private ConsumerConfig taskStatusConsumerConfig;

    /**
     * persistence store used by the framework to store its state
     */
    private String storeProvider;

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
     * <p>
     * If a store provider is not configured, a special precaution needs to be taken
     * while configuring this parameter, as this parameter defines a logical boundary between dependent tasks.
     * <pre>
     * For example:
     *      A depends on B
     *      B depends on C
     *      C depends on D
     *
     *      A is scheduled at FRI (12 AM)
     *      B is scheduled at WED (12 AM)
     *      C is scheduled at TUE (12 AM)
     *      D is scheduler at MON (12 AM)
     *
     * </pre>
     * In the above scenario the taskPurgeInterval should be a min of 6d,
     * as the first task in tree (`D`) is scheduled on MON (12 AM) and last task in the tree (`A`) will be scheduled on FRI (12 AM).
     * <p>
     * Implication of configuring taskPurgeInterval to a lower interval
     * <p>
     * Even before `A` is scheduled, the system will purge `D`, `C`, `B` tasks from memory and
     * when `A` is scheduled system will try to resolve dependency for `A` (first from in memory and
     * later from the store provider).
     * As store provider is not configured, framework won't be able to resolve the task dependency and
     * `A` will be marked as failed due to unresolved dependency
     * </p>
     */
    private String taskPurgeInterval;

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

    public ProducerConfig getTaskProducerConfig() {
        return taskProducerConfig;
    }

    public void setTaskProducerConfig(ProducerConfig taskProducerConfig) {
        this.taskProducerConfig = taskProducerConfig;
    }

    public ConsumerConfig getTaskConsumerConfig() {
        return taskConsumerConfig;
    }

    public void setTaskConsumerConfig(ConsumerConfig taskConsumerConfig) {
        this.taskConsumerConfig = taskConsumerConfig;
    }

    public ProducerConfig getTaskStatusProducerConfig() {
        return taskStatusProducerConfig;
    }

    public void setTaskStatusProducerConfig(ProducerConfig taskStatusProducerConfig) {
        this.taskStatusProducerConfig = taskStatusProducerConfig;
    }

    public ConsumerConfig getTaskStatusConsumerConfig() {
        return taskStatusConsumerConfig;
    }

    public void setTaskStatusConsumerConfig(ConsumerConfig taskStatusConsumerConfig) {
        this.taskStatusConsumerConfig = taskStatusConsumerConfig;
    }

    public String getStoreProvider() {
        return storeProvider;
    }

    public void setStoreProvider(String storeProvider) {
        this.storeProvider = storeProvider;
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
                Objects.equals(taskProducerConfig, that.taskProducerConfig) &&
                Objects.equals(taskConsumerConfig, that.taskConsumerConfig) &&
                Objects.equals(taskStatusProducerConfig, that.taskStatusProducerConfig) &&
                Objects.equals(taskStatusConsumerConfig, that.taskStatusConsumerConfig) &&
                Objects.equals(storeProvider, that.storeProvider) &&
                Objects.equals(taskPurgeInterval, that.taskPurgeInterval);
    }

    @Override
    public int hashCode() {

        return Objects.hash(readerConfig, handlerConfig, timeoutPolicyConfig, taskProducerConfig, taskConsumerConfig,
                taskStatusProducerConfig, taskStatusConsumerConfig, storeProvider, taskPurgeInterval);
    }

    @Override
    public String toString() {
        return "ApplicationConfig{" +
                "readerConfig=" + readerConfig +
                ", handlerConfig=" + handlerConfig +
                ", timeoutPolicyConfig=" + timeoutPolicyConfig +
                ", taskProducerConfig=" + taskProducerConfig +
                ", taskConsumerConfig=" + taskConsumerConfig +
                ", taskStatusProducerConfig=" + taskStatusProducerConfig +
                ", taskStatusConsumerConfig=" + taskStatusConsumerConfig +
                ", storeProvider='" + storeProvider + '\'' +
                ", taskPurgeInterval='" + taskPurgeInterval + '\'' +
                '}';
    }
}
