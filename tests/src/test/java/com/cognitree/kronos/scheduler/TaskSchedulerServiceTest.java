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

import com.cognitree.kronos.TestUtil;
import com.cognitree.kronos.executor.handlers.TestTaskHandler;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskDependencyInfo;
import com.cognitree.kronos.scheduler.store.MockTaskStore;
import com.cognitree.kronos.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;

import static com.cognitree.kronos.TestUtil.prepareDependencyInfo;
import static com.cognitree.kronos.TestUtil.sleep;
import static com.cognitree.kronos.model.Messages.FAILED_TO_RESOLVE_DEPENDENCY;
import static com.cognitree.kronos.model.Messages.TIMED_OUT;
import static com.cognitree.kronos.model.Task.Status.*;
import static com.cognitree.kronos.model.TaskDependencyInfo.Mode.all;
import static java.util.concurrent.TimeUnit.MINUTES;

@FixMethodOrder(MethodSorters.JVM)
public class TaskSchedulerServiceTest extends ApplicationTest {

    @Test
    public void testSchedulerInitialization() {
        Assert.assertEquals(SUCCESSFUL, MockTaskStore.getTask("mockTaskOne-A").getStatus());
        Assert.assertEquals(FAILED, MockTaskStore.getTask("mockTaskOne-B").getStatus());
        Assert.assertEquals(TIMED_OUT, MockTaskStore.getTask("mockTaskOne-B").getStatusMessage());

        sleep(500);
        final Task mockTaskTwo = MockTaskStore.getTask("mockTaskTwo");
        Assert.assertEquals(SUCCESSFUL, mockTaskTwo.getStatus());
        final Task mockTaskThree = MockTaskStore.getTask("mockTaskThree");
        Assert.assertEquals(FAILED, mockTaskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, mockTaskThree.getStatusMessage());
        final Task mockTaskFour = MockTaskStore.getTask("mockTaskFour");
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, mockTaskFour.getStatus());
    }

    @Test
    public void testAddTask() {
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").build();
        TaskSchedulerService.getService().schedule(taskOne);
        Assert.assertEquals(6, taskProvider.size());
        sleep(1000);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
    }

    @Test
    public void testAddDuplicateTask() {
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskOne);
        Assert.assertEquals(6, taskProvider.size());
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
    }

    @Test
    public void testTaskScheduledForExecution() {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").waitForCallback(true)
                .setCreatedAt(createdAt).build();
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .waitForCallback(true).setCreatedAt(createdAt + 5).build();

        TaskSchedulerService.getService().schedule(taskOne);
        Assert.assertTrue(taskProvider.isReadyForExecution(taskOne));
        sleep(500);
        TaskSchedulerService.getService().schedule(taskTwo);
        Assert.assertTrue(taskProvider.isReadyForExecution((taskTwo)));
        sleep(500);
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertTrue(taskTwo.getStatus().equals(RUNNING) || taskTwo.getStatus().equals(SUBMITTED));
        Assert.assertEquals(WAITING, taskThree.getStatus());
        Assert.assertFalse(taskProvider.isReadyForExecution(taskThree));
        // inform handler to finish execution of taskTwo
        TestTaskHandler.finishExecution(taskTwo.getId());
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());

        Assert.assertTrue(taskProvider.isReadyForExecution(taskThree));
        Assert.assertTrue(taskThree.getStatus().equals(RUNNING) || taskThree.getStatus().equals(SUBMITTED));
        sleep(500);
        TestTaskHandler.finishExecution(taskThree.getId());
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testTaskTimeout() {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setMaxExecutionTime("5s")
                .waitForCallback(true).setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(1000);
        Assert.assertEquals(RUNNING, taskOne.getStatus());
        sleep(5000);
        TestTaskHandler.finishExecution(taskOne.getId());
        Assert.assertEquals(FAILED, taskOne.getStatus());
        Assert.assertEquals(TIMED_OUT, taskOne.getStatusMessage());
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }

    @Test
    public void testTaskCleanup() {
        // set task created at time to a lower value than the task purge interval configured in scheduler.yaml
        final long taskPurgeInterval = DateTimeUtil.resolveDuration(schedulerConfig.getTaskPurgeInterval());
        final long createdAt = System.currentTimeMillis() - taskPurgeInterval - MINUTES.toMillis(1);
        Task independentTask = TestUtil.getTaskBuilder().setName("independentTask").setType("test")
                .waitForCallback(true).setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(independentTask);
        sleep(500);

        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").waitForCallback(true)
                .setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").waitForCallback(true)
                .setCreatedAt(createdAt).build();

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));

        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .waitForCallback(true).setCreatedAt(createdAt + 5).build();
        Task taskFour = TestUtil.getTaskBuilder().setName("taskFour").setType("test").setDependsOn(dependencyInfos)
                .waitForCallback(true).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);
        TaskSchedulerService.getService().schedule(taskFour);
        sleep(500);

        Assert.assertEquals(10, taskProvider.size());
        TaskSchedulerService.getService().deleteStaleTasks();
        // no task should be purged as they are all in RUNNING state
        Assert.assertEquals(10, taskProvider.size());
        TestTaskHandler.finishExecution(independentTask.getId());
        sleep(500);
        TaskSchedulerService.getService().deleteStaleTasks();
        Assert.assertEquals(9, taskProvider.size());
        TestTaskHandler.finishExecution(taskOne.getId());
        TestTaskHandler.finishExecution(taskTwo.getId());
        TestTaskHandler.finishExecution(taskThree.getId());
        sleep(500);
        TaskSchedulerService.getService().deleteStaleTasks();
        Assert.assertEquals(9, taskProvider.size());
        TestTaskHandler.finishExecution(taskFour.getId());
        sleep(500);
        // All tasks should be removed as they have reached the final state
        TaskSchedulerService.getService().deleteStaleTasks();
        Assert.assertEquals(5, taskProvider.size());
    }
}
