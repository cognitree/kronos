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
        scheduleExistingWorkflow();
    }

    private void scheduleExistingWorkflow() {
        // TODO load per namespace
        final List<WorkflowDefinition> workflowDefinitions = WorkflowDefinitionStoreService.getService().load();
        workflowDefinitions.forEach(workflowDefinition -> {
            logger.info("Scheduling existing workflow definition {}", workflowDefinition);
            try {
                schedule(workflowDefinition);
            } catch (Exception e) {
                logger.error("Error scheduling workflow definition {}", workflowDefinition, e);
            }
        });
    }

    @Override
    public void start() throws Exception {
        scheduler.start();
    }

    public void add(WorkflowDefinition workflowDefinition) throws Exception {
        logger.info("Received request to add workflow definition {}", workflowDefinition);
        validate(workflowDefinition);
        schedule(workflowDefinition);
    }

    /**
     * validate workflow definition
     *
     * @param workflowDefinition
     * @return
     */
    public void validate(WorkflowDefinition workflowDefinition) throws Exception {
        final HashMap<String, WorkflowTask> workflowTaskMap = new HashMap<>();
        final TopologicalSort<WorkflowTask> topologicalSort = new TopologicalSort<>();
        final List<WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        for (WorkflowTask task : workflowTasks) {
            final String taskName = task.getName();
            if (TaskDefinitionStoreService.getService().load(TaskDefinitionId.create(taskName)) == null) {
                throw new ValidationException("missing task definition with name " + taskName);
            }

            if (task.isEnabled()) {
                workflowTaskMap.put(taskName, task);
                topologicalSort.add(task);
            }
        }

        for (WorkflowTask workflowTask : workflowTasks) {
            final List<TaskDependencyInfo> dependsOn = workflowTask.getDependsOn();
            if (dependsOn != null && !dependsOn.isEmpty()) {
                for (TaskDependencyInfo taskDependencyInfo : dependsOn) {
                    final WorkflowTask dependeeTask = workflowTaskMap.get(taskDependencyInfo.getName());
                    if (dependeeTask == null) {
                        throw new ValidationException("missing task " + taskDependencyInfo.getName());
                    }

                    if (dependeeTask.getSchedule() != null && workflowTask.getSchedule() == null) {
                        throw new ValidationException("missing schedule for task " + workflowTask.getName());
                    }
                    topologicalSort.add(dependeeTask, workflowTask);
                }
            }
        }
        if (!topologicalSort.isDag()) {
            throw new ValidationException("Invalid workflow definition contains cyclic dependency)");
        }
    }

    private void schedule(WorkflowDefinition workflowDefinition) {
        if (!workflowDefinition.isEnabled()) {
            logger.warn("Workflow definition {} is disabled from scheduling", workflowDefinition);
            return;
        }

        try {
            final WorkflowDefinitionId workflowDefinitionId = workflowDefinition.getIdentity();
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("name", workflowDefinitionId.getName());
            jobDataMap.put("namespace", workflowDefinitionId.getNamespace());
            final List<WorkflowTask> workflowTasks = resolveWorkflowTasks(workflowDefinition.getTasks());
            jobDataMap.put("tasks", workflowTasks);
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

    public void execute(String workflowName, String workflowNamespace, List<WorkflowTask> workflowTasks,
                        Date nextFireTime) {
        logger.info("Executing workflow definition with name {}, namespace {}, tasks {}",
                workflowName, workflowNamespace, workflowTasks);
        final Workflow workflow = createWorkflow(workflowName, workflowNamespace);
        WorkflowStoreService.getService().store(workflow);
        logger.debug("Executing workflow {}", workflow);
        for (WorkflowTask workflowTask : workflowTasks) {
            scheduleWorkflowTask(workflowTask, workflow.getId(), workflow.getNamespace(), nextFireTime);
        }
    }

    private Workflow createWorkflow(String workflowName, String workflowNamespace) {
        final Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID().toString());
        workflow.setName(workflowName);
        workflow.setNamespace(workflowNamespace);
        workflow.setCreatedAt(System.currentTimeMillis());
        return workflow;
    }

    /**
     * sorts the workflow tasks in a topological order based on task dependency
     *
     * @param workflowTasks
     * @return
     */
    List<WorkflowTask> resolveWorkflowTasks(List<WorkflowTask> workflowTasks) {
        final HashMap<String, WorkflowTask> workflowTaskMap = new HashMap<>();
        final TopologicalSort<WorkflowTask> topologicalSort = new TopologicalSort<>();
        workflowTasks.forEach(workflowTask -> {
            workflowTaskMap.put(workflowTask.getName(), workflowTask);
            topologicalSort.add(workflowTask);
        });

        for (WorkflowTask workflowTask : workflowTasks) {
            final List<TaskDependencyInfo> dependsOn = workflowTask.getDependsOn();
            if (dependsOn != null && !dependsOn.isEmpty()) {
                dependsOn.forEach(taskDependencyInfo ->
                        topologicalSort.add(workflowTaskMap.get(taskDependencyInfo.getName()), workflowTask));
            }
        }
        return topologicalSort.sort();
    }

    private void scheduleWorkflowTask(WorkflowTask workflowTask, String workflowId,
                                      String namespace, Date nextFireTime) {
        logger.debug("scheduling workflow task {} for workflow with id {}, namespace {}, workflow next fire time {}",
                workflowTask, workflowId, namespace, nextFireTime);
        if (!workflowTask.isEnabled()) {
            logger.warn("Workflow task {} is disabled from scheduling", workflowTask);
            return;
        }
        try {
            final String taskName = workflowTask.getName();
            TaskDefinitionId taskDefinitionId = TaskDefinitionId.create(taskName);
            final TaskDefinition taskDefinition = TaskDefinitionStoreService.getService().load(taskDefinitionId);

            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("workflowTask", workflowTask);
            jobDataMap.put("taskDefinition", taskDefinition);
            jobDataMap.put("workflowId", workflowId);
            jobDataMap.put("namespace", namespace);
            JobDetail jobDetail = newJob(WorkflowTaskSchedulerJob.class)
                    .withIdentity(taskName, workflowId)
                    .usingJobData(jobDataMap)
                    .build();

            Trigger jobTrigger;
            if (workflowTask.getSchedule() != null) {
                CronScheduleBuilder workflowTaskJobSchedule = CronScheduleBuilder.cronSchedule(workflowTask.getSchedule());
                jobTrigger = newTrigger()
                        .withIdentity(taskName, workflowId)
                        .withSchedule(workflowTaskJobSchedule)
                        .endAt(nextFireTime)
                        .build();
            } else {
                jobTrigger = newTrigger()
                        .withIdentity(taskName, workflowId)
                        .startNow()
                        .build();
            }
            scheduler.scheduleJob(jobDetail, jobTrigger);
        } catch (Exception ex) {
            logger.error("Error scheduling workflow task {} for workflow with id {}, namespace {}, " +
                    "workflow end time {}", workflowTask, workflowId, namespace, nextFireTime, ex);
        }
    }

    public void update(WorkflowDefinition workflowDefinition) throws Exception {
        validate(workflowDefinition);
        // delete the old workflow definition
        delete(workflowDefinition);
        // schedule new workflow
        schedule(workflowDefinition);
    }

    public void delete(WorkflowDefinitionId workflowDefinitionId) {
        logger.info("Received request to delete workflow definition with id {}", workflowDefinitionId);
        try {
            scheduler.deleteJob(getWorkflowJobKey(workflowDefinitionId));
        } catch (SchedulerException e) {
            logger.error("Error deleting quartz job for workflow with id {}", workflowDefinitionId);
        }
    }

    Scheduler getScheduler() {
        return scheduler;
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
            final String workflowName = (String) jobDataMap.get("name");
            final String workflowNamespace = (String) jobDataMap.get("namespace");
            final List<WorkflowTask> workflowTasks = (List<WorkflowTask>) jobDataMap.get("tasks");
            try {
                WorkflowSchedulerService.getService().execute(workflowName, workflowNamespace,
                        workflowTasks, jobExecutionContext.getNextFireTime());
            } catch (Exception e) {
                logger.error("Error scheduling workflow with name {}, namespace {}, tasks {}",
                        workflowName, workflowNamespace, workflowTasks, e);
            }
        }
    }
}