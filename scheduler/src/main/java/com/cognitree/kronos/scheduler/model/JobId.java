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

package com.cognitree.kronos.scheduler.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

@JsonSerialize(as = JobId.class)
@JsonDeserialize(as = JobId.class)
public class JobId {

    private String namespace;
    private String id;
    private String workflow;

    public static JobId build(String namespace, String id, String workflowName) {
        final JobId jobId = new JobId();
        jobId.setNamespace(namespace);
        jobId.setId(id);
        jobId.setWorkflow(workflowName);
        return jobId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getWorkflow() {
        return workflow;
    }

    public void setWorkflow(String workflow) {
        this.workflow = workflow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobId)) return false;
        JobId jobId = (JobId) o;
        return Objects.equals(namespace, jobId.namespace) &&
                Objects.equals(id, jobId.id) &&
                Objects.equals(workflow, jobId.workflow);
    }

    @Override
    public int hashCode() {

        return Objects.hash(namespace, id, workflow);
    }

    @Override
    public String toString() {
        return "JobId{" +
                "namespace='" + namespace + '\'' +
                ", id='" + id + '\'' +
                ", workflow='" + workflow + '\'' +
                '}';
    }
}
