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
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinition.WorkflowTask;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
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

import static com.cognitree.kronos.model.Workflow.Status.FAILED;
import static com.cognitree.kronos.model.Workflow.Status.RUNNING;
import static com.cognitree.kronos.model.Workflow.Status.SUCCESSFUL;
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
        scheduleExistingWorkflows();
        TaskSchedulerService.getService().registerListener(new WorkflowLifecycleHandler());
    }

    private void scheduleExistingWorkflows() {
        final List<Namespace> namespaces = NamespaceService.getService().get();
        final List<WorkflowDefinition> workflowDefinitions = new ArrayList<>();
        namespaces.forEach(namespace ->
                workflowDefinitions.addAll(WorkflowDefinitionService.getService().get(namespace.getName())));
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

    void schedule(WorkflowDefinition workflowDefinition) throws SchedulerException {
        if (!workflowDefinition.isEnabled()) {
            logger.warn("Workflow definition {} is disabled from scheduling", workflowDefinition);
            return;
        }

        final WorkflowDefinitionId workflowDefinitionId = workflowDefinition.getIdentity();
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("name", workflowDefinitionId.getName());
        jobDataMap.put("namespace", workflowDefinitionId.getNamespace());
        jobDataMap.put("tasks", workflowDefinition.getTasks());
        JobDetail jobDetail = newJob(WorkflowSchedulerJob.class)
                .withIdentity(getWorkflowJobKey(workflowDefinition))
                .usingJobData(jobDataMap)
                .build();

        CronScheduleBuilder jobSchedule = getJobSchedule(workflowDefinition);
        final TriggerBuilder<CronTrigger> triggerBuilder = newTrigger()
                .withIdentity(getWorkflowTriggerKey(workflowDefinition))
                .withSchedule(jobSchedule);
        final Long startAt = workflowDefinition.getStartAt();
        if (startAt != null && startAt > 0) {
            triggerBuilder.startAt(new Date(startAt));
        }
        final Long endAt = workflowDefinition.getEndAt();
        if (endAt != null && endAt > 0) {
            triggerBuilder.endAt(new Date(endAt));
        }
        scheduler.scheduleJob(jobDetail, triggerBuilder.build());
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

    Workflow execute(String workflowName, String workflowNamespace, List<WorkflowTask> workflowTasks) {
        logger.info("Executing workflow definition with name {}, namespace {}, tasks {}",
                workflowName, workflowNamespace, workflowTasks);
        final Workflow workflow = createWorkflow(workflowName, workflowNamespace);
        WorkflowService.getService().add(workflow);
        logger.debug("Executing workflow {}", workflow);
        orderWorkflowTasks(workflowTasks).forEach(workflowTask ->
                scheduleWorkflowTask(workflowTask, workflow.getId(), workflow.getNamespace()));
        workflow.setStatus(RUNNING);
        WorkflowService.getService().update(workflow);
        return workflow;
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

    void delete(WorkflowDefinitionId workflowDefinitionId) throws SchedulerException {
        logger.info("Received request to delete workflow definition with id {}", workflowDefinitionId);
        scheduler.deleteJob(getWorkflowJobKey(workflowDefinitionId));
    }

    // used in junit
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

    public static final class WorkflowLifecycleHandler implements TaskStatusChangeListener {
        @Override
        public void statusChanged(Task task, Task.Status from, Task.Status to) {
            logger.debug("Received status change notification for task {}, from {} to {}", task, from, to);
            if (!to.isFinal()) {
                return;
            }
            final String workflowId = task.getWorkflowId();
            final String namespace = task.getNamespace();

            final List<Task> tasks = TaskService.getService().get(workflowId, namespace);
            if (tasks.isEmpty()) {
                return;
            }
            final boolean isWorkflowComplete = tasks.stream()
                    .allMatch(workflowTask -> workflowTask.getStatus().isFinal());

            if (isWorkflowComplete) {
                final boolean isSuccessful = tasks.stream()
                        .allMatch(workflowTask -> workflowTask.getStatus() == Task.Status.SUCCESSFUL);
                final Workflow workflow = WorkflowService.getService().get(WorkflowId.build(workflowId, namespace));
                workflow.setStatus(isSuccessful ? SUCCESSFUL : FAILED);
                workflow.setCompletedAt(System.currentTimeMillis());
                WorkflowService.getService().update(workflow);
            }
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
            WorkflowSchedulerService.getService().execute(workflowName, workflowNamespace, workflowTasks);
        }
    }
}