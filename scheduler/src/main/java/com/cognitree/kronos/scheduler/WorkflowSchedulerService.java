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
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.Workflow.WorkflowTask;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.util.TriggerHelper;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.cognitree.kronos.scheduler.model.Job.Status.FAILED;
import static com.cognitree.kronos.scheduler.model.Job.Status.RUNNING;
import static com.cognitree.kronos.scheduler.model.Job.Status.SUCCESSFUL;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.impl.DirectSchedulerFactory.DEFAULT_INSTANCE_ID;
import static org.quartz.impl.DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME;

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
    public void init() {
        logger.info("Initializing workflow scheduler service");
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting workflow scheduler service");
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        JobStore jobStore = storeService.getQuartzJobStore();
        SimpleThreadPool threadPool = new SimpleThreadPool(Runtime.getRuntime().availableProcessors(),
                Thread.NORM_PRIORITY);
        threadPool.setInstanceName(DEFAULT_SCHEDULER_NAME);
        threadPool.setInstanceId(DEFAULT_INSTANCE_ID);
        DirectSchedulerFactory.getInstance().createScheduler(DEFAULT_SCHEDULER_NAME, DEFAULT_INSTANCE_ID, threadPool, jobStore);
        scheduler = DirectSchedulerFactory.getInstance().getScheduler(DEFAULT_SCHEDULER_NAME);
        scheduler.start();
        TaskService.getService().registerListener(new WorkflowLifecycleHandler());

        ServiceProvider.registerService(this);
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
        final Job job = JobService.getService().create(workflow.getName(), triggerName, workflow.getNamespace());
        logger.debug("Executing workflow job {}", job);
        final List<WorkflowTask> workflowTasks = orderWorkflowTasks(workflow.getTasks());
        final List<Task> tasks = new ArrayList<>();
        for (WorkflowTask workflowTask : workflowTasks) {
            if (!workflowTask.isEnabled()) {
                logger.warn("Workflow task {} is disabled from scheduling", workflowTask);
                continue;
            }
            tasks.add(TaskService.getService().create(workflowTask, job.getId(), job.getWorkflow(), job.getNamespace()));
        }
        tasks.forEach(task -> TaskSchedulerService.getService().schedule(task));
        JobService.getService().updateStatus(job.getIdentity(), RUNNING);
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
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (Exception e) {
            logger.error("Error stopping quartz scheduler...", e);
        }
    }

    public static final class WorkflowLifecycleHandler implements TaskStatusChangeListener {
        @Override
        public void statusChanged(TaskId taskId, Task.Status from, Task.Status to) {
            logger.debug("Received status change notification for task {}, from {} to {}", taskId, from, to);
            if (!to.isFinal()) {
                return;
            }
            final String jobId = taskId.getJob();
            final String workflow = taskId.getWorkflow();
            final String namespace = taskId.getNamespace();
            try {
                final List<Task> tasks = TaskService.getService().get(jobId, workflow, namespace);
                if (tasks.isEmpty()) {
                    return;
                }
                final boolean isWorkflowComplete = tasks.stream()
                        .allMatch(workflowTask -> workflowTask.getStatus().isFinal());

                if (isWorkflowComplete) {
                    final boolean isSuccessful = tasks.stream()
                            .allMatch(workflowTask -> workflowTask.getStatus() == Task.Status.SUCCESSFUL);
                    final Job.Status status = isSuccessful ? SUCCESSFUL : FAILED;
                    JobService.getService().updateStatus(JobId.build(jobId, workflow, namespace), status);
                }
            } catch (ServiceException | ValidationException e) {
                logger.error("Error handling status change for task {}, from {} to {}", taskId, from, to, e);
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