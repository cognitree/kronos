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

package com.cognitree.tasks.scheduler;

import com.cognitree.tasks.ServiceProvider;
import com.cognitree.tasks.model.Task;
import com.cognitree.tasks.model.TaskDefinition;
import com.cognitree.tasks.scheduler.readers.TaskDefinitionReader;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * TaskSchedulerJob is scheduled by {@link TaskReaderService#scheduleJobs()} for each {@link TaskDefinitionReader} implementation.
 * <p>
 * Any change in the task definition is updated with the framework. Add new tasks definition, remove deleted one
 * and update existing task definition if required
 */
public final class TaskSchedulerJob implements org.quartz.Job {
    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulerJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        final JobDataMap jobReaderDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        final TaskDefinitionReader taskDefinitionReader = (TaskDefinitionReader) jobReaderDataMap.remove("taskDefinitionReader");
        try {
            final List<TaskDefinition> taskDefinitions = taskDefinitionReader.load();
            final String jobReaderGroup = getJobReaderGroup(jobExecutionContext);
            Map<JobKey, TaskDefinition> taskDefinitionMap = new HashMap<>();
            for (TaskDefinition taskDefinition : taskDefinitions) {
                taskDefinitionMap.put(new JobKey(taskDefinition.getId(), jobReaderGroup), taskDefinition);
            }

            updateScheduler(taskDefinitionMap, jobExecutionContext);
        } catch (Exception e) {
            logger.error("Something went wrong reading jobs", e);
            throw new JobExecutionException(e);
        }
    }

    private void updateScheduler(Map<JobKey, TaskDefinition> taskDefinitionMap,
                                 JobExecutionContext jobExecutionContext) throws Exception {
        final Scheduler scheduler = jobExecutionContext.getScheduler();
        Set<JobKey> currentlyScheduledJobKeys =
                scheduler.getJobKeys(GroupMatcher.groupEquals(getJobReaderGroup(jobExecutionContext)));
        if (logger.isTraceEnabled()) {
            logger.trace("currently scheduled jobs: {}", currentlyScheduledJobKeys);
        }

        Set<JobKey> jobsToAdd = new HashSet<>(taskDefinitionMap.keySet());

        // jobsToAdd - jobs to add
        jobsToAdd.removeAll(currentlyScheduledJobKeys);
        // jobsToUpdate - jobs to update
        Set<JobKey> jobsToUpdate = new HashSet<>(taskDefinitionMap.keySet());
        jobsToUpdate.retainAll(currentlyScheduledJobKeys);
        // jobsToRemove - jobs to remove
        Set<JobKey> jobsToRemove = new HashSet<>(currentlyScheduledJobKeys);
        jobsToRemove.removeAll(taskDefinitionMap.keySet());
        if (!jobsToAdd.isEmpty()) {
            logger.info("new jobs to add {}", jobsToAdd);
            // Add new jobs
            jobsToAdd.stream().map(taskDefinitionMap::get).distinct().forEach(jobDef ->
                    scheduleJob(jobExecutionContext, jobDef));
        }

        if (!jobsToUpdate.isEmpty()) {
            for (JobKey jobKey : jobsToUpdate) {
                final JobDetail jobDetail = scheduler.getJobDetail(jobKey);
                final TaskDefinition existingTaskDefinition =
                        (TaskDefinition) jobDetail.getJobDataMap().get("taskDefinition");
                final TaskDefinition newTaskDefinition = taskDefinitionMap.get(jobKey);
                if (!existingTaskDefinition.equals(newTaskDefinition)) {
                    logger.info("updating job: {}, from {}, to {}", jobKey, existingTaskDefinition, newTaskDefinition);
                    scheduler.deleteJob(jobKey);
                    scheduleJob(jobExecutionContext, newTaskDefinition);
                }
            }
        }

        if (!jobsToRemove.isEmpty()) {
            logger.info("jobs to remove: {}", jobsToRemove);
            // Remove obsolete jobs
            for (JobKey jobKey : jobsToRemove) {
                scheduler.deleteJob(jobKey);
            }
        }
    }

    private String getJobReaderGroup(JobExecutionContext jobExecutionContext) {
        return getIdentifier(jobExecutionContext) + "Group";
    }

    private String getIdentifier(JobExecutionContext jobExecutionContext) {
        return jobExecutionContext.getJobDetail().getKey().getName();
    }

    private void scheduleJob(JobExecutionContext jobExecutionContext, TaskDefinition taskDefinition) {
        try {
            final String jobIdentifier = taskDefinition.getId();
            final String jobGroupIdentifier = getJobReaderGroup(jobExecutionContext);

            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("taskDefinition", taskDefinition);
            JobDetail jobDetail = newJob(TaskDelegatorJob.class)
                    .withIdentity(jobIdentifier, jobGroupIdentifier)
                    .usingJobData(jobDataMap)
                    .build();

            CronScheduleBuilder jobSchedule = getJobSchedule(taskDefinition);
            Trigger simpleTrigger = newTrigger()
                    .withIdentity(jobIdentifier, jobGroupIdentifier)
                    .withSchedule(jobSchedule)
                    .build();
            jobExecutionContext.getScheduler().scheduleJob(jobDetail, simpleTrigger);
        } catch (Exception ex) {
            logger.error("Error scheduling job : {}", taskDefinition, ex);
        }
    }

    private CronScheduleBuilder getJobSchedule(TaskDefinition taskDefinition) {
        return CronScheduleBuilder.cronSchedule(taskDefinition.getSchedule());
    }

    /**
     * a task delegator job is created per task definitions and on trigger creates actual {@link Task}
     * from {@link TaskDefinition} and submits it to {@link TaskProviderService} for execution
     */
    public static final class TaskDelegatorJob implements org.quartz.Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            final JobDataMap jobHandlerDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
            logger.trace("received request to execute job with data map {}", jobHandlerDataMap.getWrappedMap());
            final TaskDefinition taskDefinition = (TaskDefinition) jobHandlerDataMap.get("taskDefinition");
            ServiceProvider.getTaskProviderService().add(taskDefinition.createTask());
        }
    }
}
