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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import static com.cognitree.kronos.scheduler.model.Job.Status.CREATED;

/**
 * runtime instance of {@link Workflow}.
 * A {@link Job} is created every time a workflow is triggered for execution.
 */
@JsonSerialize(as = Job.class)
@JsonDeserialize(as = Job.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Job extends JobId {
    private String workflow;
    private String trigger;
    private Status status = CREATED;
    private Long createdAt;
    private Long completedAt;

    public String getWorkflow() {
        return workflow;
    }

    public void setWorkflow(String workflow) {
        this.workflow = workflow;
    }

    public String getTrigger() {
        return trigger;
    }

    public void setTrigger(String trigger) {
        this.trigger = trigger;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Long completedAt) {
        this.completedAt = completedAt;
    }

    @JsonIgnore
    public JobId getIdentity() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "Job{" +
                "workflow='" + workflow + '\'' +
                ", trigger='" + trigger + '\'' +
                ", status=" + status +
                ", createdAt=" + createdAt +
                ", completedAt=" + completedAt +
                "} " + super.toString();
    }

    public enum Status {
        CREATED(false),
        RUNNING(false),
        SUCCESSFUL(true),
        FAILED(true);

        private final boolean isFinal;

        Status(boolean isFinal) {
            this.isFinal = isFinal;
        }

        public boolean isFinal() {
            return this.isFinal;
        }
    }
}
