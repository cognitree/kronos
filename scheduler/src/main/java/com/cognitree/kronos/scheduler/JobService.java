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
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.cognitree.kronos.scheduler.ValidationError.NAMESPACE_NOT_FOUND;

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

    public void add(Job job) throws ServiceException, ValidationException {
        logger.debug("Received request to add job {}", job);
        validateNamespace(job.getNamespace());
        try {
            jobStore.store(job);
        } catch (StoreException e) {
            logger.error("unable to add job {}", job, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public List<Job> get(String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all jobs under namespace {}", namespace);
        validateNamespace(namespace);
        try {
            return jobStore.load(namespace);
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
            return jobStore.load(namespace, createdAfter, createdBefore);
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
            return jobStore.loadByWorkflowName(workflowName, namespace, createdAfter, createdBefore);
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
            return jobStore.loadByWorkflowNameAndTriggerName(workflowName, triggerName, namespace, createdAfter, createdBefore);
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
        jobStore.stop();
    }
}