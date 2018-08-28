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

import com.cognitree.kronos.MockTaskBuilder;
import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.util.DateTimeUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.util.Messages.FAILED_TO_RESOLVE_DEPENDENCY;
import static com.cognitree.kronos.util.Messages.TIMED_OUT;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TaskSchedulerServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();
    private static final String TASK_TYPE = "test";

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
        // clear off any old records from topic test
        TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
    }

    @Test
    public void testAddTask() throws InterruptedException, IOException {
        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType(TASK_TYPE).build();
        Assert.assertEquals(0, TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE).size());
        TaskSchedulerService.getService().schedule(taskOne);
        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
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
        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType(TASK_TYPE).build();
        Assert.assertEquals(0, TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE).size());
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskOne);
        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
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
        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType(TASK_TYPE).build();
        TaskSchedulerService.getService().schedule(taskOne);
        Assert.assertEquals(1, taskProvider.size());

        Task taskTwo = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setType(TASK_TYPE).build();
        TaskSchedulerService.getService().schedule(taskTwo);
        Assert.assertEquals(2, taskProvider.size());

        Task taskThree = MockTaskBuilder.getTaskBuilder().setName("taskThree").setType(TASK_TYPE).build();
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(3, taskProvider.size());

        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        Assert.assertEquals(SCHEDULED, taskTwo.getStatus());
        Assert.assertEquals(SCHEDULED, taskThree.getStatus());

        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
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
        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType(TASK_TYPE).setCreatedAt(createdAt).build();
        List<String> dependsOn = new ArrayList<>();
        dependsOn.add("taskOne");
        dependsOn.add("taskTwo");
        Task taskThree = MockTaskBuilder.getTaskBuilder().setName("taskThree").setType(TASK_TYPE).setDependsOn(dependsOn)
                .setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskOne);
        TaskSchedulerService.getService().schedule(taskThree);
        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        final List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
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
        Task taskOneGroupOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setJob("workflowOne")
                .setType(TASK_TYPE).setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupOne);
        Assert.assertEquals(SCHEDULED, taskOneGroupOne.getStatus());
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskOneGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneGroupOne.getStatus());

        Task taskOneGroupTwo = MockTaskBuilder.getTaskBuilder().setName("taskOne").setJob("workflowTwo")
                .setType(TASK_TYPE).setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupTwo);
        Assert.assertEquals(SCHEDULED, taskOneGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        pushStatusUpdate(taskOneGroupTwo, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneGroupTwo.getStatus());

        Task taskTwoGroupOne = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setJob("workflowOne")
                .setType(TASK_TYPE).setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupOne);
        Assert.assertEquals(SCHEDULED, taskTwoGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupOne.getStatus());

        Task taskTwoGroupTwo = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setJob("workflowTwo")
                .setType(TASK_TYPE).setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupTwo);
        Assert.assertEquals(SCHEDULED, taskTwoGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupTwo.getStatus());

        List<String> dependsOn = new ArrayList<>();
        dependsOn.add("taskOne");
        dependsOn.add("taskTwo");

        Task taskThreeGroupOne = MockTaskBuilder.getTaskBuilder().setName("taskThree").setJob("workflowOne")
                .setType(TASK_TYPE).setDependsOn(dependsOn).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupOne);
        Assert.assertEquals(SCHEDULED, taskThreeGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskThreeGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskThreeGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThreeGroupOne.getStatus());

        Task taskThreeGroupTwo = MockTaskBuilder.getTaskBuilder().setName("taskThree").setJob("workflowTwo")
                .setType(TASK_TYPE).setDependsOn(dependsOn).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupTwo);
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(FAILED, taskThreeGroupTwo.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThreeGroupTwo.getStatusMessage());
    }

    @Test
    public void testAddTaskInDifferentNamespaceWithDependency() throws InterruptedException, IOException {
        final long createdAt = System.currentTimeMillis();
        Task taskOneGroupOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setNamespace("namespaceOne")
                .setType(TASK_TYPE).setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupOne);
        Assert.assertEquals(SCHEDULED, taskOneGroupOne.getStatus());
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskOneGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneGroupOne.getStatus());

        Task taskOneGroupTwo = MockTaskBuilder.getTaskBuilder().setName("taskOne").setNamespace("namespaceTwo")
                .setType(TASK_TYPE).setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(taskOneGroupTwo);
        Assert.assertEquals(SCHEDULED, taskOneGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        pushStatusUpdate(taskOneGroupTwo, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneGroupTwo.getStatus());

        Task taskTwoGroupOne = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setNamespace("namespaceOne")
                .setType(TASK_TYPE).setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupOne);
        Assert.assertEquals(SCHEDULED, taskTwoGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupOne.getStatus());

        Task taskTwoGroupTwo = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setNamespace("namespaceTwo")
                .setType(TASK_TYPE).setCreatedAt(createdAt + 1).build();
        TaskSchedulerService.getService().schedule(taskTwoGroupTwo);
        Assert.assertEquals(SCHEDULED, taskTwoGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupTwo.getStatus());

        List<String> dependsOn = new ArrayList<>();
        dependsOn.add("taskOne");
        dependsOn.add("taskTwo");

        Task taskThreeGroupOne = MockTaskBuilder.getTaskBuilder().setName("taskThree").setNamespace("namespaceOne")
                .setType(TASK_TYPE).setDependsOn(dependsOn).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupOne);
        Assert.assertEquals(SCHEDULED, taskThreeGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskThreeGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskThreeGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThreeGroupOne.getStatus());

        Task taskThreeGroupTwo = MockTaskBuilder.getTaskBuilder().setName("taskThree").setNamespace("namespaceTwo")
                .setType(TASK_TYPE).setDependsOn(dependsOn).setCreatedAt(createdAt + 5).build();
        TaskSchedulerService.getService().schedule(taskThreeGroupTwo);
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(FAILED, taskThreeGroupTwo.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThreeGroupTwo.getStatusMessage());
    }

    @Test
    public void testTaskTimeout() throws InterruptedException, JsonProcessingException {
        final long createdAt = System.currentTimeMillis();
        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType(TASK_TYPE).setMaxExecutionTime("500")
                .setCreatedAt(createdAt).build();
        Task taskTwo = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setType(TASK_TYPE).setCreatedAt(createdAt).build();
        List<String> dependsOn = new ArrayList<>();
        dependsOn.add("taskOne");
        dependsOn.add("taskTwo");
        Task taskThree = MockTaskBuilder.getTaskBuilder().setName("taskThree").setType(TASK_TYPE).setDependsOn(dependsOn)
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
        Task independentTask = MockTaskBuilder.getTaskBuilder().setName("independentTask")
                .setType(TASK_TYPE).setCreatedAt(createdAt).build();
        TaskSchedulerService.getService().schedule(independentTask);
        sleep(100);

        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType(TASK_TYPE)
                .setCreatedAt(createdAt).build();
        Task taskTwo = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setType(TASK_TYPE)
                .setCreatedAt(createdAt).build();

        List<String> dependsOn = new ArrayList<>();
        dependsOn.add("taskOne");
        dependsOn.add("taskTwo");

        Task taskThree = MockTaskBuilder.getTaskBuilder().setName("taskThree").setType(TASK_TYPE).setDependsOn(dependsOn)
                .setCreatedAt(createdAt + 5).build();
        Task taskFour = MockTaskBuilder.getTaskBuilder().setName("taskFour").setType(TASK_TYPE).setDependsOn(dependsOn)
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

    @Test
    public void testUpdateTaskContext() {
        MutableTask task = new MutableTask();
        final HashMap<String, Object> taskProperties = new HashMap<>();
        taskProperties.put("keyOne", "valueOne");
        taskProperties.put("keyTwo", "${taskOne.keyTwo}");
        taskProperties.put("keyThree", "${*.keyThree}");
        taskProperties.put("keyFour", "${*.keyFour}");
        taskProperties.put("keyFive", "${*.}");
        task.setProperties(taskProperties);

        final HashMap<String, Object> dependentTaskContext = new LinkedHashMap<>();
        dependentTaskContext.put("taskOne.keyTwo", "taskOneValueTwo");
        dependentTaskContext.put("taskTwo.keyTwo", "taskTwoValueTwo");
        dependentTaskContext.put("taskTwo.keyThree", "taskTwoValueThree");
        dependentTaskContext.put("taskOne.keyThree", "taskOneValueThree");
        dependentTaskContext.put("taskOne.keyFive", "valueFive");

        TaskSchedulerService.getService().updateTaskProperties(task, dependentTaskContext);

        Assert.assertEquals("taskOneValueTwo", task.getProperties().get("keyTwo"));
        Assert.assertEquals("taskOneValueThree", task.getProperties().get("keyThree"));
        Assert.assertNull(task.getProperties().get("keyFour"));
        Assert.assertNull(task.getProperties().get("keyFive"));
    }

    private void finishExecution(Task task) throws JsonProcessingException {
        pushStatusUpdate(task, SUBMITTED);
        pushStatusUpdate(task, RUNNING);
        pushStatusUpdate(task, SUCCESSFUL);
    }

    private void pushStatusUpdate(Task task, Task.Status status) throws JsonProcessingException {
        Task.TaskUpdate taskUpdate = new Task.TaskUpdate();
        taskUpdate.setTaskId(task);
        taskUpdate.setStatus(status);
        taskUpdate.setStatusMessage(task.getStatusMessage());
        TaskSchedulerService.getService().getProducer().send("taskstatus", MAPPER.writeValueAsString(taskUpdate));
    }
}
