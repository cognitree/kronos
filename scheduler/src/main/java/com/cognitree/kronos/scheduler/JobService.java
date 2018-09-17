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
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Job.Status;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.StoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.cognitree.kronos.scheduler.ValidationError.NAMESPACE_NOT_FOUND;

public class JobService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

    private final Set<JobStatusChangeListener> statusChangeListeners = new HashSet<>();
    private JobStore jobStore;

    public static JobService getService() {
        return (JobService) ServiceProvider.getService(JobService.class.getSimpleName());
    }

    @Override
    public void init() {
        logger.info("Initializing job service");
    }

    @Override
    public void start() {
        logger.info("Starting job service");
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        jobStore = storeService.getJobStore();
        ServiceProvider.registerService(this);
    }

    public Job create(String workflowName, String triggerName, String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to create job from workflow {}, trigger {} under namespace {}",
                workflowName, triggerName, namespace);
        validateNamespace(namespace);
        final Job job = new Job();
        job.setId(UUID.randomUUID().toString());
        job.setWorkflow(workflowName);
        job.setNamespace(namespace);
        job.setTrigger(triggerName);
        job.setCreatedAt(System.currentTimeMillis());
        try {
            jobStore.store(job);
        } catch (StoreException e) {
            logger.error("unable to create job from workflow {}, trigger {} under namespace {}",
                    workflowName, triggerName, namespace, e);
            throw new ServiceException(e.getMessage());
        }
        return job;
    }

    /**
     * register a listener to receive job status change notifications
     *
     * @param statusChangeListener
     */
    public void registerListener(JobStatusChangeListener statusChangeListener) {
        statusChangeListeners.add(statusChangeListener);
    }

    /**
     * deregister a job status change listener
     *
     * @param statusChangeListener
     */
    public void deregisterListener(JobStatusChangeListener statusChangeListener) {
        statusChangeListeners.remove(statusChangeListener);
    }

    public List<Job> get(String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs under namespace {}", namespace);
        validateNamespace(namespace);
        try {
            final List<Job> jobs = jobStore.load(namespace);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public Job get(JobId jobId) throws ServiceException, ValidationException {
        logger.debug("Received request to get job {}", jobId);
        validateNamespace(jobId.getNamespace());
        try {
            return jobStore.load(jobId);
        } catch (StoreException e) {
            logger.error("unable to get job {}", jobId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Job> get(String namespace, int numberOfDays) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs under namespace {} submitted in last {} number of days",
                namespace, numberOfDays);
        validateNamespace(namespace);
        long createdAfter = timeInMillisBeforeDays(numberOfDays);
        long createdBefore = System.currentTimeMillis();
        try {
            final List<Job> jobs = jobStore.load(namespace, createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs under namespace {} submitted in last {} number of days",
                    namespace, numberOfDays, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Job> get(String workflowName, String namespace, int numberOfDays) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs with workflow name {} under namespace {} submitted " +
                "in last {} number of days", workflowName, namespace, numberOfDays);
        validateNamespace(namespace);
        long createdAfter = timeInMillisBeforeDays(numberOfDays);
        long createdBefore = System.currentTimeMillis();
        try {
            final List<Job> jobs = jobStore.loadByWorkflowName(workflowName, namespace, createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs with workflow name {} under namespace {} submitted " +
                    "in last {} number of days", workflowName, namespace, numberOfDays, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Job> get(String workflowName, String triggerName, String namespace, int numberOfDays) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs with workflow name {}, trigger {} under namespace {} submitted " +
                "in last {} number of days", workflowName, triggerName, namespace, numberOfDays);
        validateNamespace(namespace);
        long createdAfter = timeInMillisBeforeDays(numberOfDays);
        long createdBefore = System.currentTimeMillis();
        try {
            final List<Job> jobs = jobStore.loadByWorkflowNameAndTriggerName(workflowName, triggerName, namespace,
                    createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs with workflow name {}, trigger {} under namespace {} submitted " +
                    "in last {} number of days", workflowName, triggerName, namespace, numberOfDays, e);
            throw new ServiceException(e.getMessage());
        }
    }

    private long timeInMillisBeforeDays(int numberOfDays) {
        final long currentTimeMillis = System.currentTimeMillis();
        return numberOfDays == -1 ? 0 : currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
    }

    public List<Task> getTasks(JobId jobId) throws ServiceException, ValidationException {
        logger.debug("Received request to get all tasks executed for job {}", jobId);
        validateNamespace(jobId.getNamespace());
        try {
            return TaskService.getService().get(jobId.getId(), jobId.getNamespace());
        } catch (ServiceException e) {
            logger.error("unable to get all tasks executed for job {}", jobId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void update(Job job) throws ServiceException, ValidationException {
        logger.debug("Received request to update job to {}", job);
        validateNamespace(job.getNamespace());
        try {
            jobStore.update(job);
        } catch (StoreException e) {
            logger.error("unable to update job to {}", job, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void updateStatus(JobId jobId, Status status) throws ServiceException, ValidationException {
        logger.debug("Received request to update job {} status to {}", jobId, status);
        try {
            final Job job = get(jobId);
            Status currentStatus = job.getStatus();
            job.setStatus(status);
            switch (status) {
                case CREATED:
                    job.setCreatedAt(System.currentTimeMillis());
                    break;
                case RUNNING:
                    break;
                case FAILED:
                case SUCCESSFUL:
                    job.setCompletedAt(System.currentTimeMillis());
                    break;
            }
            jobStore.update(job);
            notifyListeners(job, currentStatus, status);
        } catch (StoreException e) {
            logger.error("unable to update job {} status to {}", jobId, status, e);
            throw new ServiceException(e.getMessage());
        }
    }

    private void notifyListeners(Job job, Job.Status from, Job.Status to) {
        statusChangeListeners.forEach(listener -> {
            try {
                listener.statusChanged(job, from, to);
            } catch (Exception e) {
                logger.error("error notifying job status change from {}, to {} for job {}", from, to, job, e);
            }
        });
    }

    public void delete(JobId jobId) throws ServiceException, ValidationException {
        logger.debug("Received request to delete job {}", jobId);
        validateNamespace(jobId.getNamespace());
        try {
            jobStore.delete(jobId);
        } catch (StoreException e) {
            logger.error("unable to delete job {}", jobId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    private void validateNamespace(String name) throws ValidationException, ServiceException {
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            throw NAMESPACE_NOT_FOUND.createException(name);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping job service");
    }
}