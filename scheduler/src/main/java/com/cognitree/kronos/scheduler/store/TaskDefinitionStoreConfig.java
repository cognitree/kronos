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

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.ReviewPending;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

@ReviewPending
public class TaskDefinitionStoreConfig {

    private String storeClass;

    /**
     * Configuration required by the task store to instantiate itself.
     * This will be passed as an arg to the method of {@link TaskStore#init(ObjectNode)} at the time of instantiation
     */
    private ObjectNode config;

    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
    }

    public String getStoreClass() {
        return storeClass;
    }

    public void setStoreClass(String storeClass) {
        this.storeClass = storeClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskDefinitionStoreConfig)) return false;
        TaskDefinitionStoreConfig that = (TaskDefinitionStoreConfig) o;
        return Objects.equals(storeClass, that.storeClass) &&
                Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {

        return Objects.hash(storeClass, config);
    }

    @Override
    public String toString() {
        return "TaskDefinitionStoreConfig{" +
                "storeClass='" + storeClass + '\'' +
                ", config=" + config +
                '}';
    }
}
