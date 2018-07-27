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

import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.definitions.TaskDependencyInfo;
import com.cognitree.kronos.util.DateTimeUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.cognitree.kronos.model.Task.Status.*;
import static com.cognitree.kronos.model.definitions.TaskDependencyInfo.Mode.all;
import static com.cognitree.kronos.model.definitions.TaskDependencyInfo.Mode.first;
import static com.cognitree.kronos.model.definitions.TaskDependencyInfo.Mode.last;
import static com.cognitree.kronos.scheduler.TestUtil.prepareDependencyInfo;
import static com.cognitree.kronos.util.Messages.FAILED_TO_RESOLVE_DEPENDENCY;
import static com.cognitree.kronos.util.Messages.TIMED_OUT;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MINUTES;

@FixMethodOrder(MethodSorters.JVM)
public class TaskSchedulerServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();

    @BeforeClass
    public static void init() throws Exception {
        SCHEDULER_APP.start();
        TaskSchedulerService.getService().registerListener(new MockTaskStatusChangeListener());
    }

    @AfterClass
    public static void stop() {
        SCHEDULER_APP.stop();
    }

    @Before
    public void initialize() {
        // reinit will clear all the tasks from task provider
        TaskSchedulerService.getService().reinitTaskProvider();
    }

    @Test
    public void testAddTask() throws InterruptedException, IOException {
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").build();
        Assert.assertEquals(0, TaskSchedulerService.getService().getConsumer().poll(taskOne.getType()).size());
        TaskSchedulerService.getService().schedule(taskOne);
        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(taskOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        Assert.assertEquals(1, TaskSchedulerService.getService().getTaskProvider().size());
        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        finishExecution(taskOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
    }

    @Test
    public void testAddDuplicateTask() throws InterruptedException, IOException {
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").build();
        Assert.assertEquals(0, TaskSchedulerService.getService().getConsumer().poll(taskOne.getType()).size());
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskOne);
        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(taskOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        Assert.assertEquals(1, TaskSchedulerService.getService().getTaskProvider().size());
        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        finishExecution(taskOne);
        sleep(500);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
    }

    @Test
    public void testAddIndependentTasks() throws IOException, InterruptedException {
        final TaskProvider taskProvider = TaskSchedulerService.getService().getTaskProvider();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").build();
        TaskSchedulerService.getService().schedule(taskOne);
        Assert.assertEquals(1, taskProvider.size());

        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").build();
        TaskSchedulerService.getService().schedule(taskTwo);
        Assert.assertEquals(2, taskProvider.size());

        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").build();
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(3, taskProvider.size());

        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        Assert.assertEquals(SCHEDULED, taskTwo.getStatus());
        Assert.assertEquals(SCHEDULED, taskThree.getStatus());

        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(taskOne.getType());
        Assert.assertEquals(3, tasks.size());
        Assert.assertEquals(taskOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        Assert.assertEquals(taskTwo, MAPPER.readValue(tasks.get(1), MutableTask.class));
        Assert.assertEquals(taskThree, MAPPER.readValue(tasks.get(2), MutableTask.class));

        finishExecution(taskOne);
        finishExecution(taskTwo);
        finishExecution(taskThree);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void addTaskWithMissingDependency() throws InterruptedException, IOException {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(taskOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(FAILED, taskThree.getStatus());
    }

    @Test
    public void testAddTaskWithDifferentWorkflowIdWithDependency() throws InterruptedException, IOException {
        final long createdAt = System.currentTimeMillis();
        Task taskOneGroupOne = TestUtil.getTaskBuilder().setName("taskOne").setWorkflowId("workflowOne")
                .setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupOne);
        Assert.assertEquals(SCHEDULED, taskOneGroupOne.getStatus());
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(taskOneGroupOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskOneGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneGroupOne.getStatus());

        Task taskOneGroupTwo = TestUtil.getTaskBuilder().setName("taskOne").setWorkflowId("workflowTwo")
                .setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupTwo);
        Assert.assertEquals(SCHEDULED, taskOneGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskOneGroupTwo.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        pushStatusUpdate(taskOneGroupTwo, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneGroupTwo.getStatus());

        Task taskTwoGroupOne = TestUtil.getTaskBuilder().setName("taskTwo").setWorkflowId("workflowOne")
                .setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupOne);
        Assert.assertEquals(SCHEDULED, taskTwoGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskTwoGroupOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupOne.getStatus());

        Task taskTwoGroupTwo = TestUtil.getTaskBuilder().setName("taskTwo").setWorkflowId("workflowTwo")
                .setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupTwo);
        Assert.assertEquals(SCHEDULED, taskTwoGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskTwoGroupTwo.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupTwo.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));

        Task taskThreeGroupOne = TestUtil.getTaskBuilder().setName("taskThree").setWorkflowId("workflowOne")
                .setType("test").setDependsOn(dependencyInfos).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupOne);
        Assert.assertEquals(SCHEDULED, taskThreeGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskThreeGroupOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskThreeGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskThreeGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThreeGroupOne.getStatus());

        Task taskThreeGroupTwo = TestUtil.getTaskBuilder().setName("taskThree").setWorkflowId("workflowTwo")
                .setType("test").setDependsOn(dependencyInfos).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupTwo);
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskThreeGroupTwo.getType());
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(FAILED, taskThreeGroupTwo.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThreeGroupTwo.getStatusMessage());
    }

    @Test
    public void testAddTaskInDifferentNamespaceWithDependency() throws InterruptedException, IOException {
        final long createdAt = System.currentTimeMillis();
        Task taskOneGroupOne = TestUtil.getTaskBuilder().setName("taskOne").setNamespace("namespaceOne")
                .setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupOne);
        Assert.assertEquals(SCHEDULED, taskOneGroupOne.getStatus());
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(taskOneGroupOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskOneGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneGroupOne.getStatus());

        Task taskOneGroupTwo = TestUtil.getTaskBuilder().setName("taskOne").setNamespace("namespaceTwo")
                .setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupTwo);
        Assert.assertEquals(SCHEDULED, taskOneGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskOneGroupTwo.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        pushStatusUpdate(taskOneGroupTwo, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneGroupTwo.getStatus());

        Task taskTwoGroupOne = TestUtil.getTaskBuilder().setName("taskTwo").setNamespace("namespaceOne")
                .setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupOne);
        Assert.assertEquals(SCHEDULED, taskTwoGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskTwoGroupOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupOne.getStatus());

        Task taskTwoGroupTwo = TestUtil.getTaskBuilder().setName("taskTwo").setNamespace("namespaceTwo")
                .setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupTwo);
        Assert.assertEquals(SCHEDULED, taskTwoGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskTwoGroupTwo.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupTwo.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));

        Task taskThreeGroupOne = TestUtil.getTaskBuilder().setName("taskThree").setNamespace("namespaceOne")
                .setType("test").setDependsOn(dependencyInfos).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupOne);
        Assert.assertEquals(SCHEDULED, taskThreeGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskThreeGroupOne.getType());
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskThreeGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskThreeGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThreeGroupOne.getStatus());

        Task taskThreeGroupTwo = TestUtil.getTaskBuilder().setName("taskThree").setNamespace("namespaceTwo")
                .setType("test").setDependsOn(dependencyInfos).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupTwo);
        tasks = TaskSchedulerService.getService().getConsumer().poll(taskThreeGroupTwo.getType());
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(FAILED, taskThreeGroupTwo.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThreeGroupTwo.getStatusMessage());
    }

    @Test
    public void testAddTaskWithDependencyModeAll() throws InterruptedException, JsonProcessingException {
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

        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        Assert.assertEquals(SCHEDULED, taskTwo.getStatus());
        Assert.assertEquals(WAITING, taskThree.getStatus());
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(taskOne.getType());

        Assert.assertEquals(2, tasks.size());
        finishExecution(taskOne);
        finishExecution(taskTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());

        tasks = TaskSchedulerService.getService().getConsumer().poll(taskOne.getType());

        Assert.assertEquals(1, tasks.size());
        finishExecution(taskThree);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testAddTaskWithDependencyModeAllNegative() throws InterruptedException, JsonProcessingException {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);

        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        Assert.assertEquals(SCHEDULED, taskTwo.getStatus());
        Assert.assertEquals(WAITING, taskThree.getStatus());
        finishExecution(taskOne);
        pushStatusUpdate(taskTwo, FAILED);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(FAILED, taskTwo.getStatus());
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }

    @Test
    public void testAddTaskWithDependencyModeLast() throws InterruptedException, JsonProcessingException {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        Assert.assertEquals(SCHEDULED, taskOneA.getStatus());
        pushStatusUpdate(taskOneA, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        Assert.assertEquals(SCHEDULED, taskOneB.getStatus());
        pushStatusUpdate(taskOneB, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        Assert.assertEquals(SCHEDULED, taskOneC.getStatus());
        finishExecution(taskOneC);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        Assert.assertEquals(SCHEDULED, taskTwoA.getStatus());
        pushStatusUpdate(taskTwoA, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        Assert.assertEquals(SCHEDULED, taskTwoB.getStatus());
        pushStatusUpdate(taskTwoB, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        Assert.assertEquals(SCHEDULED, taskTwoC.getStatus());
        finishExecution(taskTwoC);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", last, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", last, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(SCHEDULED, taskThree.getStatus());
        finishExecution(taskThree);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testAddTaskWithDependencyModeLastNegative() throws InterruptedException, JsonProcessingException {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        Assert.assertEquals(SCHEDULED, taskOneA.getStatus());
        finishExecution(taskOneA);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        Assert.assertEquals(SCHEDULED, taskOneB.getStatus());
        finishExecution(taskOneB);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        Assert.assertEquals(SCHEDULED, taskOneC.getStatus());
        pushStatusUpdate(taskOneC, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        Assert.assertEquals(SCHEDULED, taskTwoA.getStatus());
        finishExecution(taskTwoA);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        Assert.assertEquals(SCHEDULED, taskTwoB.getStatus());
        finishExecution(taskTwoB);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        Assert.assertEquals(SCHEDULED, taskTwoC.getStatus());
        pushStatusUpdate(taskTwoC, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", last, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", last, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }

    @Test
    public void testAddTaskWithDependencyModeFirst() throws InterruptedException, JsonProcessingException {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        Assert.assertEquals(SCHEDULED, taskOneA.getStatus());
        finishExecution(taskOneA);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        Assert.assertEquals(SCHEDULED, taskOneB.getStatus());
        pushStatusUpdate(taskOneB, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        Assert.assertEquals(SCHEDULED, taskOneC.getStatus());
        pushStatusUpdate(taskOneC, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        Assert.assertEquals(SCHEDULED, taskTwoA.getStatus());
        finishExecution(taskTwoA);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        Assert.assertEquals(SCHEDULED, taskTwoB.getStatus());
        pushStatusUpdate(taskTwoB, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        Assert.assertEquals(SCHEDULED, taskTwoC.getStatus());
        pushStatusUpdate(taskTwoC, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", first, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", first, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();

        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(SCHEDULED, taskThree.getStatus());

        finishExecution(taskThree);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testAddTaskWithDependencyModeFirstNegative() throws InterruptedException, JsonProcessingException {
        final long createdAt = System.currentTimeMillis();
        Task taskOneA = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneA);
        Assert.assertEquals(SCHEDULED, taskOneA.getStatus());
        pushStatusUpdate(taskOneA, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneA.getStatus());

        Task taskOneB = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskOneB);
        Assert.assertEquals(SCHEDULED, taskOneB.getStatus());
        finishExecution(taskOneB);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneB.getStatus());

        Task taskOneC = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskOneC);
        Assert.assertEquals(SCHEDULED, taskOneC.getStatus());
        finishExecution(taskOneC);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneC.getStatus());

        Task taskTwoA = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoA);
        Assert.assertEquals(SCHEDULED, taskTwoA.getStatus());
        pushStatusUpdate(taskTwoA, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskTwoA.getStatus());

        Task taskTwoB = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoB);
        Assert.assertEquals(SCHEDULED, taskTwoB.getStatus());
        finishExecution(taskTwoB);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoB.getStatus());

        Task taskTwoC = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt + 2).build();
        TaskSchedulerService.getService().schedule(taskTwoC);
        Assert.assertEquals(SCHEDULED, taskTwoC.getStatus());
        finishExecution(taskTwoC);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoC.getStatus());

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", first, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", first, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();

        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }

    @Test
    public void testTaskTimeout() throws InterruptedException, JsonProcessingException {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test").setMaxExecutionTime("500")
                .setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test").setCreatedAt(createdAt).build();
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        pushStatusUpdate(taskOne, SUBMITTED);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);
        sleep(500);
        finishExecution(taskTwo);
        sleep(100);
        Assert.assertEquals(FAILED, taskOne.getStatus());
        Assert.assertEquals(TIMED_OUT, taskOne.getStatusMessage());
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(FAILED, taskThree.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThree.getStatusMessage());
    }

    @Test
    public void testTaskCleanup() throws InterruptedException, JsonProcessingException {
        // set task created at time to a lower value than the task purge interval configured in scheduler.yaml
        final long taskPurgeInterval = DateTimeUtil.resolveDuration("1d");
        final long createdAt = System.currentTimeMillis() - taskPurgeInterval - MINUTES.toMillis(1);
        Task independentTask = TestUtil.getTaskBuilder().setName("independentTask")
                .setType("test").setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(independentTask);
        sleep(100);

        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("test")
                .setCreatedAt(createdAt).build();
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("test")
                .setCreatedAt(createdAt).build();

        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all, "1d"));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all, "1d"));

        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        Task taskFour = TestUtil.getTaskBuilder().setName("taskFour").setType("test").setDependsOn(dependencyInfos)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);
        TaskSchedulerService.getService().schedule(taskFour);
        sleep(100);

        final TaskProvider taskProvider = TaskSchedulerService.getService().getTaskProvider();
        Assert.assertEquals(5, taskProvider.size());
        TaskSchedulerService.getService().deleteStaleTasks();
        // no task should be purged as they are all in RUNNING state
        Assert.assertEquals(5, taskProvider.size());
        finishExecution(independentTask);
        sleep(100);
        TaskSchedulerService.getService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());
        finishExecution(taskOne);
        finishExecution(taskTwo);
        finishExecution(taskThree);
        sleep(100);
        TaskSchedulerService.getService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());
        finishExecution(taskFour);
        sleep(100);
        // All tasks should be removed as they have reached the final state
        TaskSchedulerService.getService().deleteStaleTasks();
        Assert.assertEquals(0, taskProvider.size());
    }

    private void finishExecution(Task task) throws JsonProcessingException {
        pushStatusUpdate(task, SUBMITTED);
        pushStatusUpdate(task, RUNNING);
        pushStatusUpdate(task, SUCCESSFUL);
    }

    private void pushStatusUpdate(Task task, Task.Status status) throws JsonProcessingException {
        Task.TaskUpdate taskUpdate = new Task.TaskUpdate();
        taskUpdate.setTaskId(task.getId());
        taskUpdate.setWorkflowId(task.getWorkflowId());
        taskUpdate.setNamespace(task.getNamespace());
        taskUpdate.setStatus(status);
        taskUpdate.setStatusMessage(task.getStatusMessage());
        TaskSchedulerService.getService().getProducer().send("taskstatus", MAPPER.writeValueAsString(taskUpdate));
    }
}
