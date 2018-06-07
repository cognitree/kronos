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
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskDependencyInfo;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.cognitree.kronos.TestUtil.prepareDependencyInfo;
import static com.cognitree.kronos.TestUtil.sleep;
import static com.cognitree.kronos.model.Messages.FAILED_TO_RESOLVE_DEPENDENCY;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.model.TaskDependencyInfo.Mode.*;
import static java.util.concurrent.TimeUnit.HOURS;

@FixMethodOrder(MethodSorters.JVM)
public class TaskDependencyTest extends ApplicationTest {

    @Test
    public void testAddIndependentTasks() {
        final TaskProvider taskProvider = TaskSchedulerService.getService().getTaskProvider();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").build();
        TaskSchedulerService.getService().schedule(taskOne);
        Assert.assertEquals(6, taskProvider.size());
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());

        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").build();
        TaskSchedulerService.getService().schedule(taskTwo);
        Assert.assertEquals(7, taskProvider.size());
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());

        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").build();
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(8, taskProvider.size());
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void addTaskWithMissingDependency() {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();

        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(FAILED, taskThree.getStatus());
    }

    @Test
    public void testAddTaskInDifferentGroupWithDependency() {
        final long createdAt = System.currentTimeMillis();
        Task taskOneGroupOne = TestUtil.getTaskBuilder().setName("taskOne").setGroup("groupOne").setType("test")
                .setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupOne);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOneGroupOne.getStatus());

        Task taskOneGroupTwo = TestUtil.getTaskBuilder().setName("taskOne").setGroup("groupTwo").shouldPass(false)
                .setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupTwo);
        sleep(500);
        Assert.assertEquals(FAILED, taskOneGroupTwo.getStatus());

        Task taskTwoGroupOne = TestUtil.getTaskBuilder().setName("taskTwo").setGroup("groupOne").setType("test")
                .setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupOne);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupOne.getStatus());

        Task taskTwoGroupTwo = TestUtil.getTaskBuilder().setName("taskTwo").setGroup("groupTwo").setType("test")
                .setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupTwo);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupTwo.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", last, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", last, "1d"));

        Task taskThreeGroupOne = TestUtil.getTaskBuilder().setName("taskThree").setGroup("groupOne").setType("test")
                .setDependsOn(dependencyInfos).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupOne);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskThreeGroupOne.getStatus());

        Task taskThreeGroupTwo = TestUtil.getTaskBuilder().setName("taskThree").setGroup("groupTwo").setType("test")
                .setDependsOn(dependencyInfos).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupTwo);
        sleep(500);
        Assert.assertEquals(FAILED, taskThreeGroupTwo.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThreeGroupTwo.getStatusMessage());
    }

    @Test
    public void testGetDependantTask() {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test")
                .setCreatedAt(createdAt - HOURS.toMillis(1)).build();
        TaskSchedulerService.getService().schedule(taskOneA);

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneB);

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test")
                .setCreatedAt(createdAt - HOURS.toMillis(1)).build();
        TaskSchedulerService.getService().schedule(taskTwoA);

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        sleep(500);

        final TaskDependencyInfo taskOneDependencyInfo = prepareDependencyInfo("taskOne", all, "1h");
        final TaskDependencyInfo taskTwoDependencyInfo = prepareDependencyInfo("taskTwo", all, "1h");
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test")
                .setDependsOn(Arrays.asList(taskOneDependencyInfo, taskTwoDependencyInfo)).setCreatedAt(createdAt + 5)
                .build();

        final TaskProvider taskProvider = TaskSchedulerService.getService().getTaskProvider();
        final Set<Task> taskOneDependentTasks = taskProvider.getDependentTasks(taskThree, taskOneDependencyInfo);
        Assert.assertEquals(1, taskOneDependentTasks.size());
        Assert.assertEquals(taskOneB, taskOneDependentTasks.iterator().next());

        final Set<Task> taskTwoDependentTask = taskProvider.getDependentTasks(taskThree, taskTwoDependencyInfo);
        Assert.assertEquals(1, taskTwoDependentTask.size());
        Assert.assertEquals(taskTwoB, taskTwoDependentTask.iterator().next());
    }

    @Test
    public void testAddTaskWithDependencyModeAll() {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setCreatedAt(createdAt + 5)
                .setDependsOn(dependencyInfos).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testAddTaskWithDependencyModeAllNegative() {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").shouldPass(false)
                .setCreatedAt(createdAt).build();
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .shouldPass(false).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(FAILED, taskTwo.getStatus());
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }

    @Test
    public void testAddTaskWithDependencyModeLast() {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").shouldPass(false)
                .setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        sleep(500);
        Assert.assertEquals(FAILED, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        sleep(500);
        Assert.assertEquals(FAILED, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").shouldPass(false)
                .setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        sleep(500);
        Assert.assertEquals(FAILED, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        sleep(500);
        Assert.assertEquals(FAILED, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", last, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", last, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testAddTaskWithDependencyModeLastNegative() {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        sleep(500);
        Assert.assertEquals(FAILED, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        sleep(500);
        Assert.assertEquals(FAILED, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", last, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", last, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }

    @Test
    public void testAddTaskWithDependencyModeFirst() {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        sleep(500);
        Assert.assertEquals(FAILED, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        sleep(500);
        Assert.assertEquals(FAILED, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        sleep(500);
        Assert.assertEquals(FAILED, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").shouldPass(false)
                .setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        sleep(500);
        Assert.assertEquals(FAILED, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", first, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", first, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();

        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testAddTaskWithDependencyModeFirstNegative() {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").shouldPass(false)
                .setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        sleep(500);
        Assert.assertEquals(FAILED, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").shouldPass(false)
                .setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        sleep(500);
        Assert.assertEquals(FAILED, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", first, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", first, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }
}
