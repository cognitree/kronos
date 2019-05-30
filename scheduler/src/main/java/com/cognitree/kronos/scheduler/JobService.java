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
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.StoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.cognitree.kronos.scheduler.ValidationError.JOB_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.NAMESPACE_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.WORKFLOW_NOT_FOUND;

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

    Job create(String namespace, String workflowName, String triggerName) throws ServiceException, ValidationException {
        logger.info("Received request to create job from workflow {}, trigger {} under namespace {}",
                workflowName, triggerName, namespace);
        validateWorkflow(namespace, workflowName);
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
            throw new ServiceException(e.getMessage(), e.getCause());
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
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public Job get(JobId jobId) throws ServiceException, ValidationException {
        logger.debug("Received request to get job {}", jobId);
        validateWorkflow(jobId.getNamespace(), jobId.getWorkflow());
        try {
            return jobStore.load(jobId);
        } catch (StoreException e) {
            logger.error("unable to get job {}", jobId, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Job> get(String namespace, long createdAfter, long createdBefore) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs under namespace {} created between {} to {}",
                namespace, createdAfter, createdBefore);
        validateNamespace(namespace);
        try {
            final List<Job> jobs = jobStore.load(namespace, createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs under namespace {} created between {} to {}",
                    namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Job> get(String namespace, String workflowName, long createdAfter, long createdBefore) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs with workflow name {} under namespace {} created between {} to {}",
                workflowName, namespace, createdAfter, createdBefore);
        validateWorkflow(namespace, workflowName);
        try {
            final List<Job> jobs = jobStore.loadByWorkflowName(namespace, workflowName, createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs with workflow name {} under namespace {} created between {} to {}",
                    workflowName, namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Job> get(String namespace, String workflowName, String triggerName,
                         long createdAfter, long createdBefore) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs with workflow name {}, trigger {} under namespace {} " +
                "created between {} to {}", workflowName, triggerName, namespace, createdAfter, createdBefore);
        validateWorkflow(namespace, workflowName);
        try {
            final List<Job> jobs = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflowName, triggerName,
                    createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs with workflow name {}, trigger {} under namespace {} " +
                    "created between {} to {}", workflowName, triggerName, namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Job> get(String namespace, List<Status> statuses, long createdAfter, long createdBefore)
            throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs with status in {} under namespace {} " +
                "created between {} to {}", statuses, namespace, createdAfter, createdBefore);
        validateNamespace(namespace);
        try {
            final List<Job> jobs =
                    jobStore.loadByStatus(namespace, statuses, createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs with status in {} under namespace {} " +
                    "created between {} to {}", statuses, namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Job> get(String namespace, String workflowName, List<Status> statuses,
                         long createdAfter, long createdBefore) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs with workflow name {} with status in {} under namespace {} " +
                "created between {} to {}", workflowName, statuses, namespace, createdAfter, createdBefore);
        validateWorkflow(namespace, workflowName);
        try {
            final List<Job> jobs =
                    jobStore.loadByWorkflowNameAndStatus(namespace, workflowName, statuses, createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs with workflow name {} with status in {} under namespace {} " +
                    "created between {} to {}", workflowName, statuses, namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Job> get(String namespace, String workflowName, String triggerName, List<Status> statuses,
                         long createdAfter, long createdBefore) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs with workflow name {}, trigger {} with status in {} " +
                        "under namespace {} created between {} to {}",
                workflowName, triggerName, statuses, namespace, createdAfter, createdBefore);
        validateWorkflow(namespace, workflowName);
        try {
            final List<Job> jobs =
                    jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflowName, triggerName, statuses,
                            createdAfter, createdBefore);
            return jobs == null ? Collections.emptyList() : jobs;
        } catch (StoreException e) {
            logger.error("unable to get all jobs with workflow name {}, trigger {} with status in {} " +
                            "under namespace {} created between {} to {}",
                    workflowName, triggerName, statuses, namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Task> getTasks(JobId jobId) throws ServiceException, ValidationException {
        logger.debug("Received request to get all tasks executed for job {}", jobId);
        validateWorkflow(jobId.getNamespace(), jobId.getWorkflow());
        try {
            return TaskService.getService().get(jobId.getNamespace(), jobId.getId(), jobId.getWorkflow());
        } catch (ServiceException e) {
            logger.error("unable to get all tasks executed for job {}", jobId, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public Map<Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore)
            throws ValidationException, ServiceException {
        logger.debug("Received request to count jobs by status under namespace {} created " +
                "between {} to {}", namespace, createdAfter, createdBefore);
        validateNamespace(namespace);
        try {
            return jobStore.countByStatus(namespace, createdAfter, createdBefore);
        } catch (StoreException e) {
            logger.error("unable to count jobs by status under namespace {} created between {} to {}",
                    namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public Map<Status, Integer> countByStatus(String namespace, String workflowName, long createdAfter, long createdBefore)
            throws ValidationException, ServiceException {
        logger.debug("Received request to count jobs by status for workflow {} under namespace {} created " +
                "between {} to {}", workflowName, namespace, createdAfter, createdBefore);
        validateWorkflow(namespace, workflowName);
        try {
            return jobStore.countByStatusForWorkflowName(namespace, workflowName, createdAfter, createdBefore);
        } catch (StoreException e) {
            logger.error("unable to count jobs by status for workflow {} under namespace {} created " +
                    "between {} to {}", workflowName, namespace, createdAfter, createdBefore, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    void updateStatus(JobId jobId, Status status) throws ServiceException, ValidationException {
        logger.info("Received request to update job {} status to {}", jobId, status);
        validateWorkflow(jobId.getNamespace(), jobId.getWorkflow());
        try {
            final Job job = get(jobId);
            if (job == null) {
                throw JOB_NOT_FOUND.createException(jobId.getId(), jobId.getWorkflow(), jobId.getNamespace());
            }
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
            throw new ServiceException(e.getMessage(), e.getCause());
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
        logger.info("Received request to delete job {}", jobId);
        validateWorkflow(jobId.getNamespace(), jobId.getWorkflow());
        try {
            jobStore.delete(jobId);
        } catch (StoreException e) {
            logger.error("unable to delete job {}", jobId, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public void delete(String namespace, String workflowName) throws ServiceException, ValidationException {
        logger.info("Received request to delete all jobs with workflow name {} under namespace", workflowName, namespace);
        validateWorkflow(namespace, workflowName);
        try {
            TaskService.getService().deleteByWorkflowName(namespace, workflowName);
            jobStore.deleteByWorkflowName(namespace, workflowName);
        } catch (StoreException e) {
            logger.error("unable to delete all jobs with workflow name {} under namespace", workflowName, namespace, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    private void validateNamespace(String name) throws ValidationException, ServiceException {
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            throw NAMESPACE_NOT_FOUND.createException(name);
        }
    }

    private void validateWorkflow(String namespace, String workflowName) throws ServiceException, ValidationException {
        WorkflowId workflowId = WorkflowId.build(namespace, workflowName);
        if (WorkflowService.getService().get(workflowId) == null) {
            logger.error("No workflow exists with name {} under namespace {}", workflowName, namespace);
            throw WORKFLOW_NOT_FOUND.createException(workflowName, namespace);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping job service");
    }
}