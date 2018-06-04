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

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskDefinition;
import com.cognitree.kronos.scheduler.readers.TaskDefinitionReader;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * TaskDefinitionReaderJob is scheduled by {@link TaskReaderService#scheduleJobs()} for each {@link TaskDefinitionReader} implementation.
 * <p>
 * Any change in the task definition is updated with the framework. Add new tasks definition, remove deleted one
 * and update existing task definition if required
 */
public final class TaskDefinitionReaderJob implements org.quartz.Job {
    private static final Logger logger = LoggerFactory.getLogger(TaskDefinitionReaderJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        final JobDataMap jobReaderDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        final TaskDefinitionReader taskDefinitionReader =
                (TaskDefinitionReader) jobReaderDataMap.remove("taskDefinitionReader");
        try {
            final List<TaskDefinition> taskDefinitions = taskDefinitionReader.load();
            final String taskReaderGroup = getTaskReaderGroup(jobExecutionContext);
            Map<JobKey, TaskDefinition> taskDefinitionMap = new HashMap<>();
            for (TaskDefinition taskDefinition : taskDefinitions) {
                taskDefinitionMap.put(new JobKey(taskDefinition.getId(), taskReaderGroup), taskDefinition);
            }
            updateScheduler(taskDefinitionMap, jobExecutionContext);
        } catch (Exception e) {
            logger.error("Error reading task definitions", e);
            throw new JobExecutionException(e);
        }
    }

    private void updateScheduler(Map<JobKey, TaskDefinition> taskDefinitionMap,
                                 JobExecutionContext jobExecutionContext) throws Exception {
        final Scheduler scheduler = jobExecutionContext.getScheduler();
        Set<JobKey> currentlyScheduledTaskDefinitionKeys =
                scheduler.getJobKeys(GroupMatcher.groupEquals(getTaskReaderGroup(jobExecutionContext)));
        logger.trace("currently scheduled task definitions: {}", currentlyScheduledTaskDefinitionKeys);

        Set<JobKey> taskDefinitionsToAdd = new HashSet<>(taskDefinitionMap.keySet());
        // taskDefinitionsToAdd - jobs to add
        taskDefinitionsToAdd.removeAll(currentlyScheduledTaskDefinitionKeys);
        // taskDefinitionsToUpdate - jobs to update
        Set<JobKey> taskDefinitionsToUpdate = new HashSet<>(taskDefinitionMap.keySet());
        taskDefinitionsToUpdate.retainAll(currentlyScheduledTaskDefinitionKeys);
        // taskDefinitionsToRemove - jobs to remove
        Set<JobKey> taskDefinitionsToRemove = new HashSet<>(currentlyScheduledTaskDefinitionKeys);
        taskDefinitionsToRemove.removeAll(taskDefinitionMap.keySet());

        if (!taskDefinitionsToAdd.isEmpty()) {
            logger.info("adding new task definitions {}", taskDefinitionsToAdd);
            // Add new jobs
            taskDefinitionsToAdd.stream().map(taskDefinitionMap::get).distinct().forEach(jobDef ->
                    scheduleJob(jobExecutionContext, jobDef));
        }

        if (!taskDefinitionsToUpdate.isEmpty()) {
            for (JobKey jobKey : taskDefinitionsToUpdate) {
                final JobDetail jobDetail = scheduler.getJobDetail(jobKey);
                final TaskDefinition existingTaskDefinition =
                        (TaskDefinition) jobDetail.getJobDataMap().get("taskDefinition");
                final TaskDefinition newTaskDefinition = taskDefinitionMap.get(jobKey);
                if (!existingTaskDefinition.equals(newTaskDefinition)) {
                    logger.info("updating task definition {}, from {}, to {}",
                            jobKey, existingTaskDefinition, newTaskDefinition);
                    scheduler.deleteJob(jobKey);
                    scheduleJob(jobExecutionContext, newTaskDefinition);
                }
            }
        }

        if (!taskDefinitionsToRemove.isEmpty()) {
            logger.info("removing task definitions {}", taskDefinitionsToRemove);
            // Remove obsolete jobs
            for (JobKey jobKey : taskDefinitionsToRemove) {
                scheduler.deleteJob(jobKey);
            }
        }
    }

    private String getTaskReaderGroup(JobExecutionContext jobExecutionContext) {
        return getIdentifier(jobExecutionContext) + "Group";
    }

    private String getIdentifier(JobExecutionContext jobExecutionContext) {
        return jobExecutionContext.getJobDetail().getKey().getName();
    }

    private void scheduleJob(JobExecutionContext jobExecutionContext, TaskDefinition taskDefinition) {
        try {
            final String jobIdentifier = taskDefinition.getId();
            final String jobGroupIdentifier = getTaskReaderGroup(jobExecutionContext);

            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("taskDefinition", taskDefinition);
            JobDetail jobDetail = newJob(TaskSchedulerJob.class)
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
            logger.error("Error scheduling task definition {}", taskDefinition, ex);
        }
    }

    private CronScheduleBuilder getJobSchedule(TaskDefinition taskDefinition) {
        return CronScheduleBuilder.cronSchedule(taskDefinition.getSchedule());
    }

    /**
     * A task scheduler job is created per task definitions and is triggered as per the scheduled defined by
     * {@link TaskDefinition#schedule}.
     * <p>
     * On trigger, It creates an instance of {@link Task} from {@link TaskDefinition} and schedules it to
     * {@link TaskSchedulerService} for execution
     */
    public static final class TaskSchedulerJob implements org.quartz.Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            final JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
            logger.trace("received request to create task from data map {}", jobDataMap.getWrappedMap());
            final TaskDefinition taskDefinition = (TaskDefinition) jobDataMap.get("taskDefinition");
            ServiceProvider.getTaskSchedulerService().schedule(taskDefinition.createTask());
        }
    }
}
