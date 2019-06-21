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

package com.cognitree.kronos.queue;

import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.ProducerConfig;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * defines configuration required by the application to create producer and consumer to exchange message between
 * scheduler and executor.
 */
public class QueueConfig {
    private ProducerConfig producerConfig;
    private ConsumerConfig consumerConfig;
    private String taskStatusQueue;
    private String configurationQueue;
    private String controlMessageQueue;
    /**
     * time duration between successive poll to queue in millisecond, defaults to 1000ms.
     */
    private long pollIntervalInMs = TimeUnit.SECONDS.toMillis(1);

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

    public String getTaskStatusQueue() {
        return taskStatusQueue;
    }

    public void setTaskStatusQueue(String taskStatusQueue) {
        this.taskStatusQueue = taskStatusQueue;
    }

    public String getConfigurationQueue() {
        return configurationQueue;
    }

    public void setConfigurationQueue(String configurationQueue) {
        this.configurationQueue = configurationQueue;
    }

    public String getControlMessageQueue() {
        return controlMessageQueue;
    }

    public void setControlMessageQueue(String controlMessageQueue) {
        this.controlMessageQueue = controlMessageQueue;
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
        if (!(o instanceof QueueConfig)) return false;
        QueueConfig that = (QueueConfig) o;
        return pollIntervalInMs == that.pollIntervalInMs &&
                Objects.equals(producerConfig, that.producerConfig) &&
                Objects.equals(consumerConfig, that.consumerConfig) &&
                Objects.equals(taskStatusQueue, that.taskStatusQueue) &&
                Objects.equals(configurationQueue, that.configurationQueue) &&
                Objects.equals(controlMessageQueue, that.controlMessageQueue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerConfig, consumerConfig, taskStatusQueue, configurationQueue, controlMessageQueue, pollIntervalInMs);
    }

    @Override
    public String toString() {
        return "QueueConfig{" +
                "producerConfig=" + producerConfig +
                ", consumerConfig=" + consumerConfig +
                ", taskStatusQueue='" + taskStatusQueue + '\'' +
                ", configurationQueue='" + configurationQueue + '\'' +
                ", controlMessageQueue='" + controlMessageQueue + '\'' +
                ", pollIntervalInMs=" + pollIntervalInMs +
                '}';
    }
}
