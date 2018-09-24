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

package com.cognitree.kronos.queue.consumer;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * defines configuration for a {@link Consumer}.
 */
public class ConsumerConfig {

    /**
     * fully qualified class name of the {@link Consumer} implementation to be used to create a consumer.
     */
    private String consumerClass;

    /**
     * Configuration to be passed to consumer to instantiate itself.
     * This will be passed as an arg to {@link Consumer#init(ObjectNode)} at the time of instantiation.
     */
    private ObjectNode config;

    /**
     * time duration between successive poll to queue in millisecond, defaults to 1000ms.
     */
    private long pollIntervalInMs = TimeUnit.SECONDS.toMillis(1);

    public String getConsumerClass() {
        return consumerClass;
    }

    public void setConsumerClass(String consumerClass) {
        this.consumerClass = consumerClass;
    }

    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
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
        if (!(o instanceof ConsumerConfig)) return false;
        ConsumerConfig that = (ConsumerConfig) o;
        return Objects.equals(consumerClass, that.consumerClass) &&
                Objects.equals(config, that.config) &&
                Objects.equals(pollIntervalInMs, that.pollIntervalInMs);
    }

    @Override
    public int hashCode() {

        return Objects.hash(consumerClass, config, pollIntervalInMs);
    }

    @Override
    public String toString() {
        return "ConsumerConfig{" +
                "consumerClass='" + consumerClass + '\'' +
                ", config=" + config +
                ", pollIntervalInMs='" + pollIntervalInMs + '\'' +
                '}';
    }
}
