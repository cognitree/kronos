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

package com.cognitree.kronos.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

@JsonSerialize(as = MutableTaskId.class)
@JsonDeserialize(as = MutableTaskId.class)
public class MutableTaskId implements TaskId {

    private String name;
    private String job;
    private String namespace;

    public static TaskId build(String name, String jobId, String namespace) {
        final MutableTaskId mutableTaskId = new MutableTaskId();
        mutableTaskId.setName(name);
        mutableTaskId.setJob(jobId);
        mutableTaskId.setNamespace(namespace);
        return mutableTaskId;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MutableTaskId)) return false;
        MutableTaskId taskId = (MutableTaskId) o;
        return Objects.equals(name, taskId.name) &&
                Objects.equals(job, taskId.job) &&
                Objects.equals(namespace, taskId.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, job, namespace);
    }

    @Override
    public String toString() {
        return "MutableTaskId{" +
                "name='" + name + '\'' +
                ", job='" + job + '\'' +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
