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

import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.executor.handlers.MockSuccessTaskHandler;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.Scheduler;

import java.util.Collections;
import java.util.List;

import static com.cognitree.kronos.TestUtil.scheduleWorkflow;
import static com.cognitree.kronos.TestUtil.waitForTriggerToComplete;

public class TaskServiceTest {
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();
    private static final ExecutorApp EXECUTOR_APP = new ExecutorApp();

    @BeforeClass
    public static void start() throws Exception {
        SCHEDULER_APP.start();
        EXECUTOR_APP.start();
        // wait for the application to initialize itself
        Thread.sleep(100);
    }

    @AfterClass
    public static void stop() {
        SCHEDULER_APP.stop();
        EXECUTOR_APP.stop();
    }

    @Test
    public void testGetTasksByNamespace() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow("workflows/workflow-template.yaml");
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTriggerOne, scheduler);
        waitForTriggerToComplete(workflowTriggerTwo, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowOneTasks = taskService.get(workflowTriggerOne.getNamespace());
        Assert.assertEquals(3, workflowOneTasks.size());
        final List<Task> workflowTwoTasks = taskService.get(workflowTriggerTwo.getNamespace());
        Assert.assertEquals(3, workflowTwoTasks.size());
    }

    @Test
    public void testGetTasksByJob() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow("workflows/workflow-template.yaml");
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTriggerOne, scheduler);
        waitForTriggerToComplete(workflowTriggerTwo, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        JobService jobService = JobService.getService();
        TaskService taskService = TaskService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));
        final List<Task> workflowOneTasks = taskService.get(workflowTriggerOne.getNamespace(), workflowOneJobs.get(0).getId(), workflowOneJobs.get(0).getWorkflow()
        );
        Assert.assertEquals(3, workflowOneTasks.size());

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace(), 0, System.currentTimeMillis());
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
        final List<Task> workflowTwoTasks = taskService.get(workflowTriggerTwo.getNamespace(), workflowTwoJobs.get(0).getId(), workflowTwoJobs.get(0).getWorkflow()
        );
        Assert.assertEquals(3, workflowTwoTasks.size());
    }

    @Test
    public void testGetTasksByStatus() throws Exception {
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow("workflows/workflow-template.yaml");
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTriggerOne, scheduler);
        waitForTriggerToComplete(workflowTriggerTwo, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowOneTasks = taskService.get(workflowTriggerOne.getNamespace(), Collections.singletonList(Task.Status.SUCCESSFUL)
        );
        Assert.assertEquals(3, workflowOneTasks.size());

        final List<Task> workflowTwoTasks = taskService.get(workflowTriggerTwo.getNamespace(), Collections.singletonList(Task.Status.SUCCESSFUL)
        );
        Assert.assertEquals(3, workflowTwoTasks.size());
    }

    @Test
    public void testDeleteTask() throws Exception {
        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

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
        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-with-task-context.yaml");

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(3, workflowTasks.size());
        for (Task workflowTask : workflowTasks) {
            Assert.assertEquals(workflowTask.getContext(), MockSuccessTaskHandler.CONTEXT);
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
