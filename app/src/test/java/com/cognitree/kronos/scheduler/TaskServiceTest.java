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
import com.cognitree.kronos.executor.handlers.MockSuccessTaskHandler;
import com.cognitree.kronos.model.Messages;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.store.StoreService;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.TestUtil.scheduleWorkflow;
import static com.cognitree.kronos.TestUtil.waitForJobsToTriggerAndComplete;
import static com.cognitree.kronos.TestUtil.waitForTriggerToComplete;

public class TaskServiceTest extends ServiceTest {

    @Test
    public void testGetTasksByNamespace() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowOneTasks = taskService.get(workflowTriggerOne.getNamespace());
        Assert.assertEquals(3, workflowOneTasks.size());
        final List<Task> workflowTwoTasks = taskService.get(workflowTriggerTwo.getNamespace());
        Assert.assertEquals(3, workflowTwoTasks.size());
    }

    @Test
    public void testGetTasksByJob() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        JobService jobService = JobService.getService();
        TaskService taskService = TaskService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));
        final List<Task> workflowOneTasks = taskService.get(workflowTriggerOne.getNamespace(),
                workflowOneJobs.get(0).getId(), workflowOneJobs.get(0).getWorkflow());
        Assert.assertEquals(3, workflowOneTasks.size());

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
        final List<Task> workflowTwoTasks = taskService.get(workflowTriggerTwo.getNamespace(),
                workflowTwoJobs.get(0).getId(), workflowTwoJobs.get(0).getWorkflow());
        Assert.assertEquals(3, workflowTwoTasks.size());
    }

    @Test
    public void testGetTasksByStatus() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowOneTasks = taskService.get(workflowTriggerOne.getNamespace(),
                Collections.singletonList(Task.Status.SUCCESSFUL));
        Assert.assertEquals(3, workflowOneTasks.size());

        final List<Task> workflowTwoTasks = taskService.get(workflowTriggerTwo.getNamespace(),
                Collections.singletonList(Task.Status.SUCCESSFUL));
        Assert.assertEquals(3, workflowTwoTasks.size());
    }

    @Test(expected = ValidationException.class)
    public void testAbortTasksNotFound() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        waitForJobsToTriggerAndComplete(workflowTrigger);

        List<Job> jobs = JobService.getService().get(workflowTrigger.getNamespace());
        Assert.assertFalse(jobs.isEmpty());
        TaskService.getService().abortTask(TaskId.build(workflowTrigger.getNamespace(),
                UUID.randomUUID().toString(), jobs.get(0).getId(), workflowTrigger.getWorkflow()));
    }

    @Test(expected = ValidationException.class)
    public void testAbortTasksInScheduledState() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        waitForJobsToTriggerAndComplete(workflowTrigger);

        List<Job> jobs = JobService.getService().get(workflowTrigger.getNamespace());
        Assert.assertFalse(jobs.isEmpty());
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        List<Task> tasks = TaskService.getService().get(workflowTrigger.getNamespace());
        Task task = tasks.get(0);
        task.setStatus(Task.Status.SCHEDULED);
        storeService.getTaskStore().update(task);
        TaskService.getService().abortTask(task.getIdentity());
    }

    @Test
    public void testAbortTasks() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_ABORT_TASKS_YAML);

        waitForTriggerToComplete(workflowTrigger, WorkflowSchedulerService.getService().getScheduler());

        TaskService taskService = TaskService.getService();
        final List<Task> workflowTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(2, workflowTasks.size());
        Task task = workflowTasks.stream().filter(t -> t.getName().equals("taskOne")).findFirst().get();
        TaskService.getService().abortTask(task);

        waitForJobsToTriggerAndComplete(workflowTrigger);
        List<Task> tasks = taskService.get(workflowTrigger.getNamespace());
        for (Task tsk : tasks) {
            switch (tsk.getName()) {
                case "taskOne":
                    Assert.assertEquals(Task.Status.ABORTED, tsk.getStatus());
                    Assert.assertTrue(MockAbortTaskHandler.isHandled(tsk.getIdentity()));
                    break;
                case "taskTwo":
                    Assert.assertEquals(Task.Status.SKIPPED, tsk.getStatus());
                    Assert.assertEquals(Messages.ABORTED_DEPENDEE_TASK_MESSAGE, tsk.getStatusMessage());
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    @Test
    public void testDeleteTask() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowOneTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(3, workflowOneTasks.size());
        final Task taskToDelete = workflowOneTasks.get(0);
        taskService.delete(taskToDelete);
        final List<Task> workflowOneTasksPostDelete = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(2, workflowOneTasksPostDelete.size());
        Assert.assertFalse(workflowOneTasksPostDelete.contains(taskToDelete));
    }

    @Test
    public void testTaskWithContextFromDependee() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow(WORKFLOW_TEMPLATE_WITH_TASK_CONTEXT_YAML);

        waitForJobsToTriggerAndComplete(workflowTrigger);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(3, workflowTasks.size());
        for (Task workflowTask : workflowTasks) {
            Assert.assertEquals(MockSuccessTaskHandler.CONTEXT, workflowTask.getContext());
            if (workflowTask.getName().equals("taskTwo")) {
                Assert.assertEquals(1234, workflowTask.getProperties().get("keyB"));
                Assert.assertNull(workflowTask.getProperties().get("keyC"));
            }
            if (workflowTask.getName().equals("taskThree")) {
                Assert.assertEquals("abcd", workflowTask.getProperties().get("keyB"));
                Assert.assertNull(workflowTask.getProperties().get("keyC"));
            }
        }
    }
}
