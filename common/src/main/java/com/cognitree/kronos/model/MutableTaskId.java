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
    private String jobId;
    private String namespace;

    public static TaskId build(String id, String jobId, String namespace) {
        final MutableTaskId mutableTaskId = new MutableTaskId();
        mutableTaskId.setName(id);
        mutableTaskId.setJobId(jobId);
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
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
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
                Objects.equals(jobId, taskId.jobId) &&
                Objects.equals(namespace, taskId.namespace);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, jobId, namespace);
    }

    @Override
    public String toString() {
        return "MutableTaskId{" +
                "name='" + name + '\'' +
                ", jobId='" + jobId + '\'' +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
