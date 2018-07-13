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
import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.definitions.*;
import com.cognitree.kronos.model.definitions.WorkflowDefinition.WorkflowTask;
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import com.cognitree.kronos.scheduler.store.TaskDefinitionStoreService;
import com.cognitree.kronos.scheduler.store.WorkflowDefinitionStoreService;
import com.cognitree.kronos.scheduler.store.WorkflowStoreService;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * A workflow scheduler service is responsible for scheduling quartz job to execute the workflow.
 */
public final class WorkflowSchedulerService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowSchedulerService.class);

    private Scheduler scheduler;

    public static WorkflowSchedulerService getService() {
        return (WorkflowSchedulerService) ServiceProvider.getService(WorkflowSchedulerService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        scheduler = StdSchedulerFactory.getDefaultScheduler();
    }

    @Override
    public void start() throws Exception {
        scheduler.start();
    }

    public void isValid(WorkflowDefinition workflowDefinition) {
        // TODO validate workflow definition
    }

    public void schedule(WorkflowDefinition workflowDefinition) {
        try {
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("workflowDefinitionId", (WorkflowDefinitionId) workflowDefinition);
            JobDetail jobDetail = newJob(WorkflowSchedulerJob.class)
                    .withIdentity(getWorkflowJobKey(workflowDefinition))
                    .usingJobData(jobDataMap)
                    .build();

            CronScheduleBuilder jobSchedule = getJobSchedule(workflowDefinition);
            Trigger simpleTrigger = newTrigger()
                    .withIdentity(getWorkflowTriggerKey(workflowDefinition))
                    .withSchedule(jobSchedule)
                    .build();
            scheduler.scheduleJob(jobDetail, simpleTrigger);
        } catch (Exception ex) {
            logger.error("Error scheduling workflow definition {}", workflowDefinition, ex);
        }
    }

    private JobKey getWorkflowJobKey(WorkflowDefinitionId workflowDefinitionId) {
        return new JobKey(workflowDefinitionId.getName(), workflowDefinitionId.getNamespace());
    }

    private TriggerKey getWorkflowTriggerKey(WorkflowDefinitionId workflowDefinitionId) {
        return new TriggerKey(workflowDefinitionId.getName(), workflowDefinitionId.getNamespace());
    }

    private CronScheduleBuilder getJobSchedule(WorkflowDefinition workflowDefinition) {
        return CronScheduleBuilder.cronSchedule(workflowDefinition.getSchedule());
    }

    public void execute(WorkflowDefinitionId workflowDefinitionId) throws Exception {
        logger.info("Executing workflow definition with id {}", workflowDefinitionId);
        final WorkflowDefinition workflowDefinition =
                WorkflowDefinitionStoreService.getService().load(workflowDefinitionId);
        final Workflow workflow = createWorkflow(workflowDefinition);
        WorkflowStoreService.getService().store(workflow);
        logger.info("Executing workflow with id {} for name {}, namespace {}",
                workflow.getId(), workflow.getName(), workflow.getNamespace());
        final List<WorkflowTask> workflowTasks = resolveWorkflowTasks(workflowDefinition.getTasks());
        for (WorkflowTask workflowTask : workflowTasks) {
            scheduleWorkflowTask(workflowDefinition, workflow, workflowTask);
        }
    }

    private Workflow createWorkflow(WorkflowDefinition workflowDefinition) {
        final Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID().toString());
        workflow.setName(workflowDefinition.getName());
        workflow.setNamespace(workflowDefinition.getNamespace());
        workflow.setDescription(workflowDefinition.getDescription());
        workflow.setCreatedAt(System.currentTimeMillis());
        return workflow;
    }

    /**
     * sorts the workflow tasks in a topological order based on task dependency
     *
     * @param workflowTasks
     * @return
     */
    private List<WorkflowTask> resolveWorkflowTasks(List<WorkflowTask> workflowTasks) {
        final HashMap<String, WorkflowTask> workflowTaskMap = new HashMap<>();
        final TopologicalSort<WorkflowTask> topologicalSort = new TopologicalSort<>();
        workflowTasks.forEach(workflowTask -> {
            workflowTaskMap.put(workflowTask.getName(), workflowTask);
            topologicalSort.add(workflowTask);
        });

        for (WorkflowTask workflowTask : workflowTasks) {
            final List<TaskDependencyInfo> dependsOn = workflowTask.getDependsOn();
            if (dependsOn != null && !dependsOn.isEmpty()) {
                dependsOn.forEach(taskDependencyInfo -> topologicalSort.add(
                        workflowTaskMap.get(taskDependencyInfo.getName()), workflowTask));
            }
        }
        return topologicalSort.sort();
    }

    private void scheduleWorkflowTask(WorkflowDefinition workflowDefinition, Workflow workflow, WorkflowTask workflowTask) {
        try {
            TaskDefinitionId taskDefinitionId = TaskDefinitionId.create(workflowTask.getName());
            final TaskDefinition taskDefinition =
                    TaskDefinitionStoreService.getService().load(taskDefinitionId);
            final String jobIdentifier = workflowTask.getName();
            final String jobGroupIdentifier =
                    getWorkflowTaskGroup(workflow.getName(), workflow.getNamespace());

            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("workflowTask", workflowTask);
            jobDataMap.put("taskDefinition", taskDefinition);
            jobDataMap.put("workflowId", workflow.getId());
            jobDataMap.put("namespace", workflow.getNamespace());
            JobDetail jobDetail = newJob(WorkflowTaskSchedulerJob.class)
                    .withIdentity(jobIdentifier, jobGroupIdentifier)
                    .usingJobData(jobDataMap)
                    .build();

            Trigger jobTrigger;
            if (workflowTask.getSchedule() != null) {
                final CronExpression workflowSchedule = new CronExpression(workflowDefinition.getSchedule());
                CronScheduleBuilder workflowTaskJobSchedule = CronScheduleBuilder.cronSchedule(workflowTask.getSchedule());
                final Date currentDate = new Date();
                jobTrigger = newTrigger()
                        .withIdentity(jobIdentifier, jobGroupIdentifier)
                        .withSchedule(workflowTaskJobSchedule)
                        // TODO might not work for cron schedule: run at this date at this time
                        .endAt(workflowSchedule.getTimeAfter(currentDate))
                        .build();
            } else {
                jobTrigger = newTrigger()
                        .withIdentity(jobIdentifier, jobGroupIdentifier)
                        .startNow()
                        .build();
            }
            scheduler.scheduleJob(jobDetail, jobTrigger);
        } catch (Exception ex) {
            logger.error("Error scheduling workflow task {} for workflow {}", workflowTask, workflow, ex);
        }
    }

    private String getWorkflowTaskGroup(String workflowName, String namespace) {
        return workflowName + ":" + namespace;
    }

    public void delete(WorkflowDefinitionId workflowDefinitionId) {
        try {
            scheduler.deleteJob(getWorkflowJobKey(workflowDefinitionId));
        } catch (SchedulerException e) {
            logger.error("Error deleting quartz job for workflow with id {}", workflowDefinitionId);
        }
    }

    @Override
    public void stop() {
        try {
            logger.info("Stopping task reader service...");
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (Exception e) {
            logger.error("Error stopping task reader service...", e);
        }
    }

    /**
     * scheduled per task in a workflow which creates the {@link Task} instance by merging {@link WorkflowTask} and
     * {@link TaskDefinition} and schedules it to {@link TaskSchedulerService} for execution
     */
    public static final class WorkflowTaskSchedulerJob implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            final JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
            logger.trace("received request to create task from data map {}", jobDataMap.getWrappedMap());
            final TaskDefinition taskDefinition = (TaskDefinition) jobDataMap.get("taskDefinition");
            final WorkflowTask workflowTask = (WorkflowTask) jobDataMap.get("workflowTask");
            final String workflowId = (String) jobDataMap.get("workflowId");
            final String namespace = (String) jobDataMap.get("namespace");
            final Task task = createTask(workflowId, namespace, workflowTask, taskDefinition);
            TaskSchedulerService.getService().schedule(task);
        }

        private Task createTask(String workflowId, String namespace,
                                WorkflowTask workflowTask, TaskDefinition taskDefinition) {
            MutableTask task = new MutableTask();
            task.setId(UUID.randomUUID().toString());
            task.setWorkflowId(workflowId);
            task.setName(workflowTask.getName());
            task.setNamespace(namespace);
            task.setType(taskDefinition.getType());
            task.setMaxExecutionTime(workflowTask.getMaxExecutionTime());
            task.setTimeoutPolicy(workflowTask.getTimeoutPolicy());
            task.setDependsOn(workflowTask.getDependsOn());
            final HashMap<String, Object> taskProperties = new HashMap<>();
            taskProperties.putAll(taskDefinition.getProperties());
            taskProperties.putAll(workflowTask.getProperties());
            task.setProperties(taskProperties);
            task.setCreatedAt(System.currentTimeMillis());
            return task;
        }
    }

    /**
     * quartz job scheduled per workflow definition and submits the workflow definition for execution
     */
    public static final class WorkflowSchedulerJob implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            final JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
            logger.trace("received request to execute workflow with data map {}", jobDataMap.getWrappedMap());
            final WorkflowDefinitionId workflowDefinitionId = (WorkflowDefinitionId) jobDataMap.get("workflowDefinitionId");
            try {
                WorkflowSchedulerService.getService().execute(workflowDefinitionId);
            } catch (Exception e) {
                logger.error("Error scheduling workflow with id {}", workflowDefinitionId, e);
            }
        }
    }
}