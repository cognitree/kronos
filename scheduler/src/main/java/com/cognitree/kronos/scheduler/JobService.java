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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.Job;
import com.cognitree.kronos.model.JobId;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class JobService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

    private final StoreConfig storeConfig;
    private JobStore jobStore;

    public JobService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static JobService getService() {
        return (JobService) ServiceProvider.getService(JobService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        logger.info("Initializing job service");
        jobStore = (JobStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        jobStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {
        logger.info("Starting job service");
    }

    public void add(Job job) {
        logger.debug("Received request to add job {}", job);
        jobStore.store(job);
    }

    public List<Job> get(String namespace) {
        logger.debug("Received request to get all jobs under namespace {}", namespace);
        return jobStore.load(namespace);
    }

    public Job get(JobId jobId) {
        logger.debug("Received request to get job {}", jobId);
        return jobStore.load(jobId);
    }

    public List<Job> get(String namespace, int numberOfDays) {
        logger.debug("Received request to get all jobs under namespace {} submitted in last {} number of days",
                namespace, numberOfDays);
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        return jobStore.load(namespace, createdAfter, createdBefore);
    }

    public List<Job> get(String workflowName, String namespace, int numberOfDays) {
        logger.debug("Received request to get all jobs with workflow name {} under namespace {} submitted " +
                "in last {} number of days", workflowName, namespace, numberOfDays);
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        return jobStore.loadByWorkflowName(workflowName, namespace, createdAfter, createdBefore);
    }

    public List<Job> get(String workflowName, String triggerName, String namespace, int numberOfDays) {
        logger.debug("Received request to get all jobs with workflow name {}, trigger {} under namespace {} submitted " +
                "in last {} number of days", workflowName, triggerName, namespace, numberOfDays);
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        return jobStore.loadByWorkflowNameAndTrigger(workflowName, triggerName, namespace, createdAfter, createdBefore);
    }

    public List<Task> getTasks(JobId jobId) {
        logger.debug("Received request to get all tasks executed for job {}", jobId);
        return TaskService.getService().get(jobId.getId(), jobId.getNamespace());
    }

    public void update(Job job) {
        logger.debug("Received request to update job to {}", job);
        jobStore.update(job);
    }

    public void delete(JobId jobId) {
        logger.debug("Received request to delete job {}", jobId);
        jobStore.delete(jobId);
    }

    @Override
    public void stop() {
        logger.info("Stopping job service");
        jobStore.stop();
    }
}