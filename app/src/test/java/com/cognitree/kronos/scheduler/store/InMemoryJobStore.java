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

import com.cognitree.kronos.model.Job;
import com.cognitree.kronos.model.JobId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryJobStore implements JobStore {

    private final Map<JobId, Job> jobs = new HashMap<>();

    @Override
    public List<Job> load(String namespace, long createdAfter, long createdBefore) {
        final ArrayList<Job> jobs = new ArrayList<>();
        this.jobs.values().forEach(job -> {
            if (job.getNamespace().equals(namespace) && job.getCreatedAt() > createdAfter
                    && job.getCreatedAt() < createdBefore) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    @Override
    public List<Job> loadByWorkflowName(String workflowName, String namespace, long createdAfter, long createdBefore) {
        final ArrayList<Job> jobs = new ArrayList<>();
        this.jobs.values().forEach(job -> {
            if (job.getWorkflow().equals(workflowName) && job.getNamespace().equals(namespace)
                    && job.getCreatedAt() > createdAfter && job.getCreatedAt() < createdBefore) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerName(String workflowName, String triggerName, String namespace,
                                                      long createdAfter, long createdBefore) {
        final ArrayList<Job> jobs = new ArrayList<>();
        this.jobs.values().forEach(job -> {
            if (job.getWorkflow().equals(workflowName) && job.getTrigger().equals(triggerName)
                    && job.getNamespace().equals(namespace) && job.getCreatedAt() > createdAfter
                    && job.getCreatedAt() < createdBefore) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    @Override
    public void init(ObjectNode storeConfig) {

    }

    @Override
    public void store(Job job) throws StoreException {
        final JobId jobId = JobId.build(job.getId(), job.getNamespace());
        if (jobs.containsKey(jobId)) {
            throw new StoreException("already exists");
        } else {
            jobs.put(jobId, job);
        }
    }

    @Override
    public List<Job> load(String namespace) {
        final ArrayList<Job> jobs = new ArrayList<>();
        this.jobs.values().forEach(job -> {
            if (job.getNamespace().equals(namespace)) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    @Override
    public Job load(JobId jobId) {
        return jobs.get(JobId.build(jobId.getId(), jobId.getNamespace()));
    }

    @Override
    public void update(Job job) throws StoreException {
        final JobId jobId = JobId.build(job.getId(), job.getNamespace());
        if (!jobs.containsKey(jobId)) {
            throw new StoreException("does not exists");
        }
        jobs.put(jobId, job);
    }

    @Override
    public void delete(JobId jobId) throws StoreException {
        final JobId buildJobId = JobId.build(jobId.getId(), jobId.getNamespace());
        if (jobs.remove(buildJobId) == null) {
            throw new StoreException("does not exists");
        }
    }

    @Override
    public void stop() {

    }
}
