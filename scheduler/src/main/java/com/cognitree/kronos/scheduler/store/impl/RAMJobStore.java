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

package com.cognitree.kronos.scheduler.store.impl;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cognitree.kronos.scheduler.model.Job.Status;

public class RAMJobStore implements JobStore {
    private static final Logger logger = LoggerFactory.getLogger(RAMJobStore.class);

    private final Map<JobId, Job> jobs = new HashMap<>();

    @Override
    public void store(Job job) throws StoreException {
        logger.debug("Received request to store job {}", job);
        final JobId jobId = JobId.build(job.getId(), job.getWorkflow(), job.getNamespace());
        if (jobs.containsKey(jobId)) {
            throw new StoreException("job with id " + jobId + " already exists");
        } else {
            jobs.put(jobId, job);
        }
    }

    @Override
    public List<Job> load(String namespace) {
        logger.debug("Received request to get all jobs under namespace {}", namespace);
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
        logger.debug("Received request to get job with id {}", jobId);
        return jobs.get(JobId.build(jobId.getId(), jobId.getWorkflow(), jobId.getNamespace()));
    }

    @Override
    public List<Job> load(String namespace, long createdAfter, long createdBefore) {
        logger.debug("Received request to get all jobs under namespace {}, created after {}, created before {}",
                namespace, createdAfter, createdBefore);
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
        logger.debug("Received request to get jobs with workflow name {}, namespace {}, created after {}, created before {}",
                workflowName, namespace, createdAfter, createdBefore);
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
        logger.debug("Received request to get all jobs with workflow name {} under namespace {}, triggerName {}," +
                " created after {}, created before {}", workflowName, namespace, triggerName, createdAfter, createdBefore);
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
    public List<Job> loadByStatusIn(List<Status> statuses, String namespace, long createdAfter, long createdBefore) {
        logger.debug("Received request to get all jobs having status in {} under namespace {}" +
                " created after {}, created before {}", statuses, namespace, createdAfter, createdBefore);
        final ArrayList<Job> jobs = new ArrayList<>();
        this.jobs.values().forEach(job -> {
            if (statuses.contains(job.getStatus()) && job.getNamespace().equals(namespace)
                    && job.getCreatedAt() > createdAfter && job.getCreatedAt() < createdBefore) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    @Override
    public List<Job> loadByWorkflowNameAndStatusIn(String workflowName, List<Status> statuses, String namespace,
                                                   long createdAfter, long createdBefore) {
        logger.debug("Received request to get all jobs with workflow name {} having status in {} under namespace {}" +
                " created after {}, created before {}", workflowName, statuses, namespace, createdAfter, createdBefore);
        final ArrayList<Job> jobs = new ArrayList<>();
        this.jobs.values().forEach(job -> {
            if (job.getWorkflow().equals(workflowName) && statuses.contains(job.getStatus()) && job.getNamespace().equals(namespace)
                    && job.getCreatedAt() > createdAfter && job.getCreatedAt() < createdBefore) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerNameAndStatusIn(String workflowName, String triggerName,
                                                                 List<Status> statuses, String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get all jobs with workflow name {}, trigger name {} having status in {}" +
                        " under namespace {} created after {}, created before {}",
                workflowName, triggerName, statuses, namespace, createdAfter, createdBefore);
        final ArrayList<Job> jobs = new ArrayList<>();
        this.jobs.values().forEach(job -> {
            if (job.getWorkflow().equals(workflowName) && job.getTrigger().equals(triggerName)
                    && job.getNamespace().equals(namespace) && statuses.contains(job.getStatus())
                    && job.getCreatedAt() > createdAfter && job.getCreatedAt() < createdBefore) {
                jobs.add(job);
            }
        });
        return jobs;
    }

    @Override
    public Map<Status, Integer> groupByStatus(String namespace, long createdAfter, long createdBefore) {
        logger.debug("Received request to group by status jobs under namespace {}, created after {}, created before {}",
                namespace, createdAfter, createdBefore);
        Map<Status, Integer> statusMap = new HashMap<>();
        for (Status status : Status.values()) {
            statusMap.put(status, 0);
        }
        for (Job job : this.jobs.values()) {
            if (job.getNamespace().equals(namespace) && job.getCreatedAt() > createdAfter
                    && job.getCreatedAt() < createdBefore) {
                statusMap.put(job.getStatus(), statusMap.get(job.getStatus()) + 1);
            }
        }
        return statusMap;
    }

    @Override
    public Map<Status, Integer> groupByStatusForWorkflowName(String workflowName, String namespace, long createdAfter, long createdBefore) {
        logger.debug("Received request to group by status jobs with workflow name {}, namespace {}, created after {}, " +
                "created before {}", workflowName, namespace, createdAfter, createdBefore);
        Map<Status, Integer> statusMap = new HashMap<>();
        for (Status status : Status.values()) {
            statusMap.put(status, 0);
        }
        for (Job job : this.jobs.values()) {
            if (job.getWorkflow().equals(workflowName) && job.getNamespace().equals(namespace)
                    && job.getCreatedAt() > createdAfter && job.getCreatedAt() < createdBefore) {
                statusMap.put(job.getStatus(), statusMap.get(job.getStatus()) + 1);
            }
        }
        return statusMap;
    }

    @Override
    public void update(Job job) throws StoreException {
        logger.info("Received request to update job to {}", job);
        final JobId jobId = JobId.build(job.getId(), job.getWorkflow(), job.getNamespace());
        if (!jobs.containsKey(jobId)) {
            throw new StoreException("job with id " + jobId + " does not exists");
        }
        jobs.put(jobId, job);
    }

    @Override
    public void delete(JobId jobId) throws StoreException {
        logger.debug("Received request to delete job with id {}", jobId);
        final JobId buildJobId = JobId.build(jobId.getId(), jobId.getWorkflow(), jobId.getNamespace());
        if (jobs.remove(buildJobId) == null) {
            throw new StoreException("job with id " + jobId + " does not exists");
        }
    }

    @Override
    public void deleteByWorkflowName(String workflowName, String namespace) {
        logger.debug("Received request to delete job with workflow name {} under namespace {}", workflowName, namespace);
        final List<Job> jobs = loadByWorkflowName(workflowName, namespace, 0, System.currentTimeMillis());
        jobs.forEach(this.jobs::remove);
    }
}
