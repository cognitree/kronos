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

package com.cognitree.kronos.response;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Objects;

@JsonSerialize(as = JobResponse.class)
@JsonDeserialize(as = JobResponse.class)
public class JobResponse extends Job {
    private List<Task> tasks;

    public static JobResponse create(Job job, List<Task> tasks) {
        JobResponse jobResponse = new JobResponse();
        jobResponse.setTasks(tasks);
        jobResponse.setId(job.getId());
        jobResponse.setWorkflow(job.getWorkflow());
        jobResponse.setTrigger(job.getTrigger());
        jobResponse.setNamespace(job.getNamespace());
        jobResponse.setStatus(job.getStatus());
        jobResponse.setCreatedAt(job.getCreatedAt());
        jobResponse.setCompletedAt(job.getCompletedAt());
        return jobResponse;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobResponse)) return false;
        if (!super.equals(o)) return false;
        JobResponse that = (JobResponse) o;
        return Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), tasks);
    }

    @Override
    public String toString() {
        return "JobResponse{" +
                "tasks=" + tasks +
                "} " + super.toString();
    }
}
