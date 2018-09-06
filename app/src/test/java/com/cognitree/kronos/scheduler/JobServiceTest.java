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

import com.cognitree.kronos.ApplicationTest;
import com.cognitree.kronos.executor.handlers.MockFailureTaskHandler;
import com.cognitree.kronos.executor.handlers.MockTaskHandler;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Messages;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.policies.MockTimeoutPolicy;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.quartz.Scheduler;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JobServiceTest extends ApplicationTest {

    @Test
    public void testGetAllJobsByNamespace() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow("workflows/workflow-template.yaml");
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for both the job to be triggered
        waitForTriggerToComplete(workflowTriggerOne, scheduler);
        waitForTriggerToComplete(workflowTriggerTwo, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace());
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace());
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
    }

    @Test
    public void testGetAllJobsCreatedInLastNDays() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow("workflows/workflow-template.yaml");
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for both the job to be triggered
        waitForTriggerToComplete(workflowTriggerOne, scheduler);
        waitForTriggerToComplete(workflowTriggerTwo, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerOne.getNamespace(), 10);
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
    }

    @Test
    public void testGetAllJobsByWorkflow() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow("workflows/workflow-template.yaml");
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for both the job to be triggered
        waitForTriggerToComplete(workflowTriggerOne, scheduler);
        waitForTriggerToComplete(workflowTriggerTwo, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getWorkflow(),
                workflowTriggerOne.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getWorkflow(),
                workflowTriggerTwo.getNamespace(), 10);
        Assert.assertEquals(1, workflowTwoJobs.size());
    }

    @Test
    public void testGetAllJobsByWorkflowAndTrigger() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow("workflows/workflow-template.yaml");
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for both the job to be triggered
        waitForTriggerToComplete(workflowTriggerOne, scheduler);
        waitForTriggerToComplete(workflowTriggerTwo, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getWorkflow(), workflowTriggerOne.getName(),
                workflowTriggerOne.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getWorkflow(), workflowTriggerTwo.getName(),
                workflowTriggerTwo.getNamespace(), 10);
        Assert.assertEquals(1, workflowTwoJobs.size());
    }

    @Test
    public void testGetJobTasks() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for the job to be triggered
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getWorkflow(), workflowTrigger.getName(),
                workflowTrigger.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());

        final List<Task> tasks = jobService.getTasks(workflowOneJobs.get(0));
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    break;
                case "taskTwo":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    break;
                case "taskThree":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testGetJobTasksFailedDueToTimeout() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-timeout-tasks.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for the job to be triggered
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getWorkflow(), workflowTrigger.getName(),
                workflowTrigger.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = JobService.getService().getTasks(job);
        Assert.assertEquals(3, tasks.size());
        Task taskThree = null;
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    break;
                case "taskTwo":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    break;
                case "taskThree":
                    taskThree = task;
                    Assert.assertEquals(Task.Status.FAILED, task.getStatus());
                    Assert.assertEquals(Messages.TIMED_OUT, task.getStatusMessage());
                    Assert.assertTrue(MockTimeoutPolicy.getTimeoutTasks().contains(task));
                    break;
                default:
                    Assert.fail();
            }
        }
        Assert.assertNotNull(taskThree);
        // Right now timed out tasks are not cleaned up on the executor side
        // thus finishing the task execution manually
        MockTaskHandler.finishExecution(taskThree.getName(), taskThree.getJob(), taskThree.getNamespace());
    }

    @Test
    public void testGetJobTasksFailedDueToHandler() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-failed-handler.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for the job to be triggered
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getWorkflow(), workflowTrigger.getName(),
                workflowTrigger.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = JobService.getService().getTasks(job);
        Assert.assertEquals(3, tasks.size());
        Task taskTwo = null;
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    break;
                case "taskTwo":
                    taskTwo = task;
                    Assert.assertEquals(Task.Status.FAILED, task.getStatus());
                    break;
                case "taskThree":
                    Assert.assertEquals(Task.Status.FAILED, task.getStatus());
                    Assert.assertEquals(Messages.FAILED_TO_RESOLVE_DEPENDENCY, task.getStatusMessage());
                    break;
                default:
                    Assert.fail();
            }
        }
        Assert.assertNotNull(taskTwo);
        Assert.assertTrue(MockFailureTaskHandler.isHandled(taskTwo.getName(), taskTwo.getJob(), taskTwo.getNamespace()));
    }

    @Test
    public void testDeleteJob() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        // wait for both the job to be triggered
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getWorkflow(), workflowTrigger.getName(),
                workflowTrigger.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());
        final Job workflowOneJob = workflowOneJobs.get(0);
        Assert.assertEquals(workflowOneJob.getStatus(), Job.Status.SUCCESSFUL);
        jobService.delete(workflowOneJob);

        final List<Job> workflowOneJobsPostDelete = jobService.get(workflowTrigger.getWorkflow(), workflowTrigger.getName(),
                workflowTrigger.getNamespace(), 10);
        Assert.assertEquals(0, workflowOneJobsPostDelete.size());
    }
}
