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

public class WorkflowStatistics {
    private JobExecutionCounters jobs;
    private TaskExecutionCounters tasks;
    private long from;
    private long to;

    public JobExecutionCounters getJobs() {
        return jobs;
    }

    public void setJobs(JobExecutionCounters jobs) {
        this.jobs = jobs;
    }

    public TaskExecutionCounters getTasks() {
        return tasks;
    }

    public void setTasks(TaskExecutionCounters tasks) {
        this.tasks = tasks;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "WorkflowStatistics{" +
                "jobs=" + jobs +
                ", tasks=" + tasks +
                ", from=" + from +
                ", to=" + to +
                '}';
    }
}
