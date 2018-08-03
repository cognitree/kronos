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

import java.util.Objects;

public class MutableTaskId implements TaskId {

    private String id;
    private String workflowId;
    private String namespace;

    public static TaskId create(String id, String workflowId, String namespace) {
        final MutableTaskId mutableTaskId = new MutableTaskId();
        mutableTaskId.setId(id);
        mutableTaskId.setWorkflowId(workflowId);
        mutableTaskId.setNamespace(namespace);
        return mutableTaskId;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
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
        if (o == null || getClass() != o.getClass()) return false;
        MutableTaskId that = (MutableTaskId) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(workflowId, that.workflowId) &&
                Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, workflowId, namespace);
    }

    @Override
    public String toString() {
        return "MutableTaskId{" +
                "id='" + id + '\'' +
                ", workflowId='" + workflowId + '\'' +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
