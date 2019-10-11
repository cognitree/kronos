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
import com.cognitree.kronos.executor.handlers.MockAbortTaskHandler;
import com.cognitree.kronos.executor.handlers.MockFailureTaskHandler;
import com.cognitree.kronos.executor.handlers.MockRetryTaskHandler;
import com.cognitree.kronos.executor.handlers.MockSuccessTaskHandler;
import com.cognitree.kronos.model.Messages;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.store.StoreService;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.TestUtil.scheduleWorkflow;
import static com.cognitree.kronos.TestUtil.waitForJobsToTriggerAndComplete;
import static com.cognitree.kronos.TestUtil.waitForTaskToBeRunning;
import static com.cognitree.kronos.TestUtil.waitForTriggerToComplete;
import static com.cognitree.kronos.model.Task.Status.ABORTED;
import static com.cognitree.kronos.model.Task.Status.SKIPPED;

public class JobServiceTest extends ServiceTest {

    @Test
    public void testGetAllJobsByNamespace() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

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
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace());
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerOne.getNamespace());
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
    }

    @Test
    public void testGetAllJobsByWorkflow() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace(), workflowTriggerOne.getWorkflow(),
                0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace(), workflowTriggerTwo.getWorkflow(),
                0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowTwoJobs.size());
    }

    @Test
    public void testGetAllJobsByWorkflowAndTrigger() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace(), workflowTriggerOne.getWorkflow(),
                workflowTriggerOne.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace(), workflowTriggerTwo.getWorkflow(),
                workflowTriggerTwo.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowTwoJobs.size());
    }

    @Test
    public void testGetJobTasks() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final List<Task> tasks = jobService.getTasks(workflowOneJobs.get(0));
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskTwo":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskThree":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testGetJobTasksFailedDueToTimeout() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_TIMEOUT_TASKS_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = jobService.getTasks(job);
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskTwo":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskThree":
                    Assert.assertEquals(Task.Status.TIMED_OUT, task.getStatus());
                    Assert.assertEquals(Messages.TIMED_OUT_EXECUTING_TASK_MESSAGE, task.getStatusMessage());
                    Assert.assertTrue(MockAbortTaskHandler.isHandled(task.getIdentity()));
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testGetJobTasksFailedDueToTimeoutWithRetry() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_TIMEOUT_TASKS_WITH_RETRY_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = jobService.getTasks(job);
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskTwo":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertEquals(2, task.getRetryCount());
                    Assert.assertTrue(MockRetryTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskThree":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testGetJobTasksFailedDueToHandler() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_FAILED_HANDLER_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = jobService.getTasks(job);
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskTwo":
                    Assert.assertEquals(Task.Status.FAILED, task.getStatus());
                    Assert.assertEquals(3, task.getRetryCount());
                    Assert.assertTrue(MockFailureTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskThree":
                    Assert.assertEquals(SKIPPED, task.getStatus());
                    Assert.assertEquals(Messages.FAILED_DEPENDEE_TASK_MESSAGE, task.getStatusMessage());
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testConditionSuccessJob() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(CONDITION_WORKFLOW_TEMPLATE_SUCCESS_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = jobService.getTasks(job);
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                case "taskTwo":
                case "taskThree":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    Assert.assertEquals(0, task.getRetryCount());
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testLastConditionFailsJob() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(CONDITION_WORKFLOW_TEMPLATE_FAILURE_LASTCONDITION_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = jobService.getTasks(job);
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                case "taskTwo":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskThree":
                    Assert.assertEquals(SKIPPED, task.getStatus());
                    Assert.assertEquals(Messages.TASK_SKIPPED_CONDITION_FAILS, task.getStatusMessage());
                    Assert.assertEquals(0, task.getRetryCount());
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testSecondConditionFails() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(CONDITION_WORKFLOW_TEMPLATE_FAILURE_SECONDCONDITION_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());

        final Job job = workflowOneJobs.get(0);
        final List<Task> tasks = jobService.getTasks(job);
        Assert.assertEquals(3, tasks.size());
        for (Task task : tasks) {
            switch (task.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.SUCCESSFUL, task.getStatus());
                    Assert.assertTrue(MockSuccessTaskHandler.isHandled(task.getIdentity()));
                    break;
                case "taskTwo":
                    Assert.assertEquals(SKIPPED, task.getStatus());
                    Assert.assertEquals(Messages.TASK_SKIPPED_CONDITION_FAILS, task.getStatusMessage());
                    Assert.assertEquals(0, task.getRetryCount());
                    break;
                case "taskThree":
                    Assert.assertEquals(SKIPPED, task.getStatus());
                    Assert.assertEquals(Messages.SKIPPED_DEPENDEE_TASK_MESSAGE, task.getStatusMessage());
                    Assert.assertEquals(0, task.getRetryCount());
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test(expected = ValidationException.class)
    public void testAbortJobNotFound() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        waitForJobsToTriggerAndComplete(workflowTrigger);

        List<Job> jobs = JobService.getService().get(workflowTrigger.getNamespace());
        Assert.assertFalse(jobs.isEmpty());
        JobService.getService().abortJob(JobId.build(workflowTrigger.getNamespace(),
                UUID.randomUUID().toString(), workflowTrigger.getWorkflow()));
    }

    @Test
    public void testAbortJob() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_ABORT_TASKS_YAML);

        waitForTriggerToComplete(workflowTrigger, WorkflowSchedulerService.getService().getScheduler());

        Task taskOne = TaskService.getService().get(workflowTrigger.getNamespace())
                .stream().filter(task -> task.getName().equals("taskOne")).findFirst().get();
        waitForTaskToBeRunning(taskOne);

        List<Job> jobs = JobService.getService().get(workflowTrigger.getNamespace());
        JobService.getService().abortJob(jobs.get(0).getIdentity());

        waitForJobsToTriggerAndComplete(workflowTrigger);
        List<Task> tasks = TaskService.getService().get(workflowTrigger.getNamespace());
        for (Task tsk : tasks) {
            switch (tsk.getName()) {
                case "taskOne":
                    Assert.assertEquals(ABORTED, tsk.getStatus());
                    Assert.assertTrue(MockAbortTaskHandler.isHandled(tsk.getIdentity()));
                    break;
                case "taskTwo":
                    Assert.assertTrue((tsk.getStatus() == ABORTED) ||
                            (tsk.getStatus() == SKIPPED));
                    break;
                default:
                    Assert.fail();
            }
        }

        Assert.assertEquals(Job.Status.FAILED,
                JobService.getService().get(workflowTrigger.getNamespace()).get(0).getStatus());
    }

    @Test(expected = ValidationException.class)
    public void testAbortJobWithTaskInScheduledState() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        waitForJobsToTriggerAndComplete(workflowTrigger);

        List<Job> jobs = JobService.getService().get(workflowTrigger.getNamespace());
        Assert.assertFalse(jobs.isEmpty());
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        List<Task> tasks = TaskService.getService().get(workflowTrigger.getNamespace());
        // move back the job to running state and task to scheduled state
        JobService.getService().updateStatus(jobs.get(0).getIdentity(), Job.Status.RUNNING);
        Task task = tasks.get(0);
        task.setStatus(Task.Status.SCHEDULED);
        storeService.getTaskStore().update(task);
        JobService.getService().abortJob(jobs.get(0));
    }

    @Test
    public void testDeleteJob() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());
        final Job workflowOneJob = workflowOneJobs.get(0);
        Assert.assertEquals(Job.Status.SUCCESSFUL, workflowOneJob.getStatus());
        jobService.delete(workflowOneJob);

        final List<Job> workflowOneJobsPostDelete = jobService.get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                workflowTrigger.getName(), 0, System.currentTimeMillis());
        Assert.assertEquals(0, workflowOneJobsPostDelete.size());
    }
}
