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
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.Workflow.WorkflowTask;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import com.cognitree.kronos.scheduler.util.TriggerHelper;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.scheduler.model.Job.Status.FAILED;
import static com.cognitree.kronos.scheduler.model.Job.Status.RUNNING;
import static com.cognitree.kronos.scheduler.model.Job.Status.SUCCESSFUL;
import static org.quartz.JobBuilder.newJob;

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
        TaskSchedulerService.getService().registerListener(new WorkflowLifecycleHandler());
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting workflow scheduler service");
        scheduler.start();
    }

    void schedule(Workflow workflow, WorkflowTrigger workflowTrigger) throws SchedulerException, ParseException {
        if (!workflowTrigger.isEnabled()) {
            logger.warn("Workflow trigger {} is disabled from scheduling", workflowTrigger);
            return;
        }

        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("namespace", workflow.getNamespace());
        jobDataMap.put("workflowName", workflow.getName());
        jobDataMap.put("triggerName", workflowTrigger.getName());
        JobDetail jobDetail = newJob(WorkflowSchedulerJob.class)
                .withIdentity(getJobKey(workflowTrigger))
                .usingJobData(jobDataMap)
                .build();

        final Trigger trigger = TriggerHelper.buildTrigger(workflowTrigger, getTriggerKey(workflowTrigger));
        scheduler.scheduleJob(jobDetail, trigger);
    }

    private JobKey getJobKey(WorkflowTriggerId workflowTriggerId) {
        return new JobKey(workflowTriggerId.getName(),
                getGroup(workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace()));
    }

    private TriggerKey getTriggerKey(WorkflowTriggerId workflowTriggerId) {
        return new TriggerKey(workflowTriggerId.getName(),
                getGroup(workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace()));
    }

    private String getGroup(String workflowName, String namespace) {
        return workflowName + ":" + namespace;
    }

    private Job execute(Workflow workflow, String triggerName) throws ServiceException, ValidationException {
        logger.info("Received request to execute workflow {} by trigger {}", workflow, triggerName);
        final Job job = createWorkflowJob(workflow.getName(), workflow.getNamespace(), triggerName);
        JobService.getService().add(job);
        logger.debug("Executing workflow {}", job);
        for (WorkflowTask workflowTask : orderWorkflowTasks(workflow.getTasks())) {
            scheduleWorkflowTask(workflowTask, job.getId(), job.getNamespace());
        }
        job.setStatus(RUNNING);
        JobService.getService().update(job);
        return job;
    }

    private Job createWorkflowJob(String workflowName, String workflowNamespace, String trigger) {
        final Job job = new Job();
        job.setId(UUID.randomUUID().toString());
        job.setWorkflow(workflowName);
        job.setNamespace(workflowNamespace);
        job.setTrigger(trigger);
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

    private void scheduleWorkflowTask(WorkflowTask workflowTask, String workflowId,
                                      String namespace) throws ServiceException {
        logger.debug("scheduling workflow task {} for workflow with id {}, namespace {}",
                workflowTask, workflowId, namespace);
        if (!workflowTask.isEnabled()) {
            logger.warn("Workflow task {} is disabled from scheduling", workflowTask);
            return;
        }

        final Task task = createTask(workflowId, workflowTask, namespace);
        TaskSchedulerService.getService().schedule(task);
    }

    private Task createTask(String workflowId, WorkflowTask workflowTask, String namespace) {
        MutableTask task = new MutableTask();
        task.setName(UUID.randomUUID().toString());
        task.setJob(workflowId);
        task.setName(workflowTask.getName());
        task.setNamespace(namespace);
        task.setType(workflowTask.getType());
        task.setMaxExecutionTime(workflowTask.getMaxExecutionTime());
        task.setTimeoutPolicy(workflowTask.getTimeoutPolicy());
        task.setDependsOn(workflowTask.getDependsOn());
        task.setProperties(workflowTask.getProperties());
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
            final String jobId = task.getJob();
            final String namespace = task.getNamespace();
            try {
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
            } catch (ServiceException | ValidationException e) {
                logger.error("Error handling status change for task {}, from {} to {}", task, from, to, e);
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
            final String namespace = jobDataMap.getString("namespace");
            final String workflowName = jobDataMap.getString("workflowName");
            final String triggerName = jobDataMap.getString("triggerName");
            try {
                final WorkflowId workflowId = WorkflowId.build(workflowName, namespace);
                final Workflow workflow = WorkflowService.getService().get(workflowId);
                WorkflowSchedulerService.getService().execute(workflow, triggerName);
            } catch (ServiceException | ValidationException e) {
                logger.error("Error executing workflow {} for trigger {}", workflowName, triggerName, e);
            }
        }
    }
}