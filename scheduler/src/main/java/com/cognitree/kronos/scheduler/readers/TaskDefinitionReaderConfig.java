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

package com.cognitree.kronos.scheduler.readers;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * defines configuration for a {@link TaskDefinitionReader}
 */
public class TaskDefinitionReaderConfig {

    /**
     * fully qualified class name of the {@link TaskDefinitionReader} implementation
     */
    private String readerClass;

    /**
     * Configuration to be passed to reader to instantiate itself.
     * This will be passed as an arg to the {@link TaskDefinitionReader#init(ObjectNode)} method at the time of instantiation
     */
    private ObjectNode config;

    /**
     * A cron string representing how frequently the framework should look for new task from {@link TaskDefinitionReader}
     */
    private String schedule;

    public String getReaderClass() {
        return readerClass;
    }

    public void setReaderClass(String readerClass) {
        this.readerClass = readerClass;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
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
        if (!(o instanceof TaskDefinitionReaderConfig)) return false;
        TaskDefinitionReaderConfig that = (TaskDefinitionReaderConfig) o;
        return Objects.equals(readerClass, that.readerClass) &&
                Objects.equals(config, that.config) &&
                Objects.equals(schedule, that.schedule);
    }

    @Override
    public int hashCode() {

        return Objects.hash(readerClass, config, schedule);
    }

    @Override
    public String toString() {
        return "TaskDefinitionReaderConfig{" +
                "readerClass='" + readerClass + '\'' +
                ", config=" + config +
                ", schedule='" + schedule + '\'' +
                '}';
    }
}
