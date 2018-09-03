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
import com.cognitree.kronos.model.Job;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.WorkflowTrigger;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.quartz.Scheduler;

import java.util.Collections;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TaskServiceTest extends ApplicationTest {

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
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace(), 10);
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));
        final List<Task> workflowOneTasks = taskService.get(workflowOneJobs.get(0).getId(),
                workflowTriggerOne.getNamespace());
        Assert.assertEquals(3, workflowOneTasks.size());

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace(), 10);
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
        final List<Task> workflowTwoTasks = taskService.get(workflowTwoJobs.get(0).getId(),
                workflowTriggerTwo.getNamespace());
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
        final List<Task> workflowOneTasks = taskService.get(Collections.singletonList(Task.Status.SUCCESSFUL),
                workflowTriggerOne.getNamespace());
        Assert.assertEquals(3, workflowOneTasks.size());

        final List<Task> workflowTwoTasks = taskService.get(Collections.singletonList(Task.Status.SUCCESSFUL),
                workflowTriggerTwo.getNamespace());
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
}
