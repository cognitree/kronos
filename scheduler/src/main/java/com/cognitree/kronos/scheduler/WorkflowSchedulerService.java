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
import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.Workflow.WorkflowTask;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.model.WorkflowTrigger;
import com.cognitree.kronos.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.model.Job.Status.FAILED;
import static com.cognitree.kronos.model.Job.Status.RUNNING;
import static com.cognitree.kronos.model.Job.Status.SUCCESSFUL;
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
        logger.info("Initializing workflow scheduler service");
        scheduler = StdSchedulerFactory.getDefaultScheduler();
        scheduleExistingWorkflows();
        TaskSchedulerService.getService().registerListener(new WorkflowLifecycleHandler());
    }

    private void scheduleExistingWorkflows() {
        final List<Namespace> namespaces = NamespaceService.getService().get();
        final List<WorkflowTrigger> workflowTriggers = new ArrayList<>();
        namespaces.forEach(namespace ->
                workflowTriggers.addAll(WorkflowTriggerService.getService().get(namespace.getName())));
        workflowTriggers.forEach(workflowTrigger -> {
            logger.info("Scheduling existing workflow trigger {}", workflowTrigger);
            try {
                schedule(workflowTrigger);
            } catch (Exception e) {
                logger.error("Error scheduling workflow trigger {}", workflowTrigger, e);
            }
        });
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting workflow scheduler service");
        scheduler.start();
    }

    void schedule(WorkflowTrigger workflowTrigger) throws SchedulerException {
        if (!workflowTrigger.isEnabled()) {
            logger.warn("Workflow trigger {} is disabled from scheduling", workflowTrigger);
            return;
        }

        final WorkflowId workflowId =
                WorkflowId.build(workflowTrigger.getWorkflowName(), workflowTrigger.getNamespace());

        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("workflowId", workflowId);
        jobDataMap.put("trigger", workflowTrigger);
        JobDetail jobDetail = newJob(WorkflowSchedulerJob.class)
                .withIdentity(getJobKey(workflowTrigger))
                .usingJobData(jobDataMap)
                .build();

        CronScheduleBuilder jobSchedule = getJobSchedule(workflowTrigger);
        final TriggerBuilder<CronTrigger> triggerBuilder = newTrigger()
                .withIdentity(getTriggerKey(workflowTrigger))
                .withSchedule(jobSchedule);
        final long startAt = workflowTrigger.getStartAt();
        if (startAt > 0) {
            triggerBuilder.startAt(new Date(startAt));
        }
        final long endAt = workflowTrigger.getEndAt();
        if (endAt > 0) {
            triggerBuilder.endAt(new Date(endAt));
        }
        scheduler.scheduleJob(jobDetail, triggerBuilder.build());
    }

    private JobKey getJobKey(WorkflowTriggerId workflowTriggerId) {
        return new JobKey(workflowTriggerId.getWorkflowName() + ":" + workflowTriggerId.getName(),
                workflowTriggerId.getNamespace());
    }

    private TriggerKey getTriggerKey(WorkflowTriggerId workflowTriggerId) {
        return new TriggerKey(workflowTriggerId.getWorkflowName() + ":" + workflowTriggerId.getName(),
                workflowTriggerId.getNamespace());
    }

    private CronScheduleBuilder getJobSchedule(WorkflowTrigger workflowTrigger) {
        return CronScheduleBuilder.cronSchedule(workflowTrigger.getSchedule());
    }

    Job execute(Workflow workflow, WorkflowTrigger workflowTrigger) {
        logger.info("Received request to execute workflow {} by trigger {}", workflow, workflowTrigger);
        final Job job = createWorkflowJob(workflow.getName(), workflow.getNamespace(), workflowTrigger.getName());
        JobService.getService().add(job);
        logger.debug("Executing workflow {}", job);
        orderWorkflowTasks(workflow.getTasks()).forEach(workflowTask ->
                scheduleWorkflowTask(workflowTask, job.getId(), job.getNamespace()));
        job.setStatus(RUNNING);
        JobService.getService().update(job);
        return job;
    }

    private Job createWorkflowJob(String workflowName, String workflowNamespace, String trigger) {
        final Job job = new Job();
        job.setId(UUID.randomUUID().toString());
        job.setWorkflowName(workflowName);
        job.setNamespace(workflowNamespace);
        job.setTriggerName(trigger);
        job.setCreatedAt(System.currentTimeMillis());
        return job;
    }

    /**
     * sorts the workflow tasks in a topological order based on task dependency
     *
     * @param workflowTasks
     * @return
     */
    List<WorkflowTask> orderWorkflowTasks(List<WorkflowTask> workflowTasks) {
        final HashMap<String, WorkflowTask> workflowTaskMap = new HashMap<>();
        final TopologicalSort<WorkflowTask> topologicalSort = new TopologicalSort<>();
        workflowTasks.forEach(workflowTask -> {
            workflowTaskMap.put(workflowTask.getName(), workflowTask);
            topologicalSort.add(workflowTask);
        });

        for (WorkflowTask workflowTask : workflowTasks) {
            final List<String> dependsOn = workflowTask.getDependsOn();
            if (dependsOn != null && !dependsOn.isEmpty()) {
                dependsOn.forEach(dependentTask ->
                        topologicalSort.add(workflowTaskMap.get(dependentTask), workflowTask));
            }
        }
        return topologicalSort.sort();
    }

    private void scheduleWorkflowTask(WorkflowTask workflowTask, String workflowId, String namespace) {
        logger.debug("scheduling workflow task {} for workflow with id {}, namespace {}",
                workflowTask, workflowId, namespace);
        if (!workflowTask.isEnabled()) {
            logger.warn("Workflow task {} is disabled from scheduling", workflowTask);
            return;
        }

        TaskDefinitionId taskDefinitionId = TaskDefinitionId.build(workflowTask.getTaskDefinitionName());
        final TaskDefinition taskDefinition = TaskDefinitionService.getService().get(taskDefinitionId);
        final Task task = createTask(workflowId, workflowTask, taskDefinition, namespace);
        TaskSchedulerService.getService().schedule(task);
    }

    private Task createTask(String workflowId, WorkflowTask workflowTask,
                            TaskDefinition taskDefinition, String namespace) {
        MutableTask task = new MutableTask();
        task.setName(UUID.randomUUID().toString());
        task.setJobId(workflowId);
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

    void delete(WorkflowTriggerId workflowTriggerId) throws SchedulerException {
        logger.info("Received request to delete workflow trigger {}", workflowTriggerId);
        scheduler.deleteJob(getJobKey(workflowTriggerId));
    }

    // used in junit
    Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow scheduler service");
        try {
            logger.info("Stopping task reader service...");
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (Exception e) {
            logger.error("Error stopping task reader service...", e);
        }
    }

    public static final class WorkflowLifecycleHandler implements TaskStatusChangeListener {
        @Override
        public void statusChanged(Task task, Task.Status from, Task.Status to) {
            logger.debug("Received status change notification for task {}, from {} to {}", task, from, to);
            if (!to.isFinal()) {
                return;
            }
            final String jobId = task.getJobId();
            final String namespace = task.getNamespace();

            final List<Task> tasks = TaskService.getService().get(jobId, namespace);
            if (tasks.isEmpty()) {
                return;
            }
            final boolean isWorkflowComplete = tasks.stream()
                    .allMatch(workflowTask -> workflowTask.getStatus().isFinal());

            if (isWorkflowComplete) {
                final boolean isSuccessful = tasks.stream()
                        .allMatch(workflowTask -> workflowTask.getStatus() == Task.Status.SUCCESSFUL);
                final Job job = JobService.getService().get(JobId.build(jobId, namespace));
                job.setStatus(isSuccessful ? SUCCESSFUL : FAILED);
                job.setCompletedAt(System.currentTimeMillis());
                JobService.getService().update(job);
            }
        }
    }

    /**
     * quartz job scheduled per workflow and submits the workflow for execution
     */
    public static final class WorkflowSchedulerJob implements org.quartz.Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            final JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
            logger.trace("received request to execute workflow with data map {}", jobDataMap.getWrappedMap());
            final WorkflowId workflowId = (WorkflowId) jobDataMap.get("workflowId");
            final WorkflowTrigger trigger = (WorkflowTrigger) jobDataMap.get("trigger");
            // TODO optimization: do not load every workflow every time it is trigger
            final Workflow workflow = WorkflowService.getService().get(workflowId);
            WorkflowSchedulerService.getService().execute(workflow, trigger);
        }
    }
}