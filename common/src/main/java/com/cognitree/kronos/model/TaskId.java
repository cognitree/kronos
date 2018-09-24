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

@JsonSerialize(as = TaskId.class)
@JsonDeserialize(as = TaskId.class)
public class TaskId {

    private String namespace;
    private String name;
    private String job;
    private String workflow;

    public static TaskId build(String namespace, String name, String jobId, String workflowName) {
        final TaskId taskId = new TaskId();
        taskId.setNamespace(namespace);
        taskId.setName(name);
        taskId.setJob(jobId);
        taskId.setWorkflow(workflowName);
        return taskId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
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
        if (!(o instanceof TaskId)) return false;
        TaskId taskId = (TaskId) o;
        return Objects.equals(namespace, taskId.namespace) &&
                Objects.equals(name, taskId.name) &&
                Objects.equals(job, taskId.job) &&
                Objects.equals(workflow, taskId.workflow);
    }

    @Override
    public int hashCode() {

        return Objects.hash(namespace, name, job, workflow);
    }

    @Override
    public String toString() {
        return "TaskId{" +
                "namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", job='" + job + '\'' +
                ", workflow='" + workflow + '\'' +
                '}';
    }
}
