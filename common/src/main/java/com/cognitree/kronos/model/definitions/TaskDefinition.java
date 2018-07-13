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

package com.cognitree.kronos.model.definitions;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A task definition holds set of attributes required by the task during execution.
 */
public class TaskDefinition extends TaskDefinitionId {

    private String type;
    private Map<String, Object> properties = new HashMap<>();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    public TaskDefinitionId getIdentity() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskDefinition)) return false;
        TaskDefinition that = (TaskDefinition) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, type, properties);
    }

    @Override
    public String toString() {
        return "TaskDefinition{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", properties=" + properties +
                '}';
    }
}
