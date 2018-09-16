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
import com.cognitree.kronos.model.TaskUpdate;
import com.cognitree.kronos.util.DateTimeUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.scheduler.model.Messages.FAILED_TO_RESOLVE_DEPENDENCY;
import static com.cognitree.kronos.scheduler.model.Messages.TIMED_OUT;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TaskSchedulerServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TASK_TYPE = "test";
    private SchedulerApp schedulerApp;

    @Before
    public void start() throws Exception {
        schedulerApp = new SchedulerApp();
        schedulerApp.start();
    }

    @After
    public void stop() {
        schedulerApp.stop();
    }

    @Test
    public void testAddTask() throws InterruptedException, IOException {
        String namespace = UUID.randomUUID().toString();
        String jobId = UUID.randomUUID().toString();
        final TaskProvider taskProvider = TaskSchedulerService.getService().getTaskProvider();
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskOne);
        Assert.assertEquals(1, taskProvider.size());

        Task taskTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();

        TaskSchedulerService.getService().schedule(taskTwo);
        Assert.assertEquals(2, taskProvider.size());

        Task taskThree = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
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
    public void testAddDuplicateTask() throws InterruptedException, IOException {
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(UUID.randomUUID().toString())
                .setNamespace(UUID.randomUUID().toString())
                .setType(TASK_TYPE)
                .build();
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
    public void testAddTaskWithMissingDependency() throws InterruptedException, IOException {
        String namespace = UUID.randomUUID().toString();
        String jobId = UUID.randomUUID().toString();
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        List<String> dependsOn = new ArrayList<>();
        dependsOn.add(taskOne.getName());
        dependsOn.add("taskTwo");
        Task taskThree = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setDependsOn(dependsOn)
                .build();
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
    public void testAddTaskInWorkflowSameNamespace() throws InterruptedException, IOException {
        String namespace = UUID.randomUUID().toString();
        String jobOne = UUID.randomUUID().toString();
        Task taskOneJobOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobOne)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskOneJobOne);
        Assert.assertEquals(SCHEDULED, taskOneJobOne.getStatus());
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneJobOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskOneJobOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneJobOne.getStatus());

        Task taskTwoJobOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobOne)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskTwoJobOne);
        Assert.assertEquals(SCHEDULED, taskTwoJobOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoJobOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoJobOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoJobOne.getStatus());

        List<String> dependsOnJobOne = new ArrayList<>();
        dependsOnJobOne.add(taskOneJobOne.getName());
        dependsOnJobOne.add(taskTwoJobOne.getName());
        Task taskThreeGroupOne = MockTaskBuilder.getTaskBuilder().setName(UUID.randomUUID().toString())
                .setJob(jobOne)
                .setNamespace(namespace)
                .setDependsOn(dependsOnJobOne)
                .setType(TASK_TYPE)
                .build();

        TaskSchedulerService.getService().schedule(taskThreeGroupOne);
        Assert.assertEquals(SCHEDULED, taskThreeGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskThreeGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskThreeGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThreeGroupOne.getStatus());

        String jobTwo = UUID.randomUUID().toString();
        Task taskOneGroupTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(jobTwo)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskOneGroupTwo);
        Assert.assertEquals(SCHEDULED, taskOneGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        pushStatusUpdate(taskOneGroupTwo, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneGroupTwo.getStatus());

        Task taskTwoGroupTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(jobTwo)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskTwoGroupTwo);
        Assert.assertEquals(SCHEDULED, taskTwoGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupTwo.getStatus());

        List<String> dependsOnJobTwo = new ArrayList<>();
        dependsOnJobTwo.add(taskOneJobOne.getName());
        dependsOnJobTwo.add(taskTwoJobOne.getName());
        Task taskThreeGroupTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(jobTwo)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setDependsOn(dependsOnJobTwo)
                .build();
        TaskSchedulerService.getService().schedule(taskThreeGroupTwo);
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(FAILED, taskThreeGroupTwo.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThreeGroupTwo.getStatusMessage());
    }

    @Test
    public void testAddTaskInDifferentNamespace() throws InterruptedException, IOException {
        String namespaceOne = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        Task taskOneJobOne = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespaceOne)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskOneJobOne);
        Assert.assertEquals(SCHEDULED, taskOneJobOne.getStatus());
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneJobOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskOneJobOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOneJobOne.getStatus());

        Task taskTwoJobOne = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespaceOne)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskTwoJobOne);
        Assert.assertEquals(SCHEDULED, taskTwoJobOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoJobOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoJobOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoJobOne.getStatus());

        List<String> dependsOnJobOne = new ArrayList<>();
        dependsOnJobOne.add(taskOneJobOne.getName());
        dependsOnJobOne.add(taskTwoJobOne.getName());
        Task taskThreeGroupOne = MockTaskBuilder.getTaskBuilder().setName(UUID.randomUUID().toString())
                .setJob(job)
                .setNamespace(namespaceOne)
                .setDependsOn(dependsOnJobOne)
                .setType(TASK_TYPE)
                .build();

        TaskSchedulerService.getService().schedule(taskThreeGroupOne);
        Assert.assertEquals(SCHEDULED, taskThreeGroupOne.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskThreeGroupOne, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskThreeGroupOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskThreeGroupOne.getStatus());

        String namespaceTwo = UUID.randomUUID().toString();
        Task taskOneGroupTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespaceTwo)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskOneGroupTwo);
        Assert.assertEquals(SCHEDULED, taskOneGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskOneGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        pushStatusUpdate(taskOneGroupTwo, FAILED);
        sleep(100);
        Assert.assertEquals(FAILED, taskOneGroupTwo.getStatus());

        Task taskTwoGroupTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespaceTwo)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskTwoGroupTwo);
        Assert.assertEquals(SCHEDULED, taskTwoGroupTwo.getStatus());
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(taskTwoGroupTwo, MAPPER.readValue(tasks.get(0), MutableTask.class));
        finishExecution(taskTwoGroupTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwoGroupTwo.getStatus());

        List<String> dependsOnJobTwo = new ArrayList<>();
        dependsOnJobTwo.add(taskOneJobOne.getName());
        dependsOnJobTwo.add(taskTwoJobOne.getName());
        Task taskThreeGroupTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespaceTwo)
                .setType(TASK_TYPE)
                .setDependsOn(dependsOnJobTwo)
                .build();
        TaskSchedulerService.getService().schedule(taskThreeGroupTwo);
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(FAILED, taskThreeGroupTwo.getStatus());
        Assert.assertEquals(FAILED_TO_RESOLVE_DEPENDENCY, taskThreeGroupTwo.getStatusMessage());
    }

    @Test
    public void testTaskTimeout() throws InterruptedException, JsonProcessingException {
        String namespace = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setMaxExecutionTime("500")
                .build();
        Task taskTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        List<String> dependsOn = new ArrayList<>();
        dependsOn.add(taskOne.getName());
        dependsOn.add(taskTwo.getName());
        Task taskThree = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setDependsOn(dependsOn)
                .build();
        TaskSchedulerService.getService().schedule(taskOne);
        pushStatusUpdate(taskOne, SUBMITTED);
        TaskSchedulerService.getService().schedule(taskTwo);
        TaskSchedulerService.getService().schedule(taskThree);
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(2, tasks.size());

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
        String namespace = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        // set task created at time to a lower value than the task purge interval configured in scheduler.yaml
        final long taskPurgeInterval = DateTimeUtil.resolveDuration("1d");
        final long createdAt = System.currentTimeMillis() - taskPurgeInterval - MINUTES.toMillis(1);
        Task independentTask = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setCreatedAt(createdAt)
                .build();
        TaskSchedulerService.getService().schedule(independentTask);
        sleep(100);
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setCreatedAt(createdAt)
                .build();
        Task taskTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setCreatedAt(createdAt)
                .build();

        List<String> dependsOn = new ArrayList<>();
        dependsOn.add(taskOne.getName());
        dependsOn.add(taskTwo.getName());

        Task taskThree = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setCreatedAt(createdAt)
                .setDependsOn(dependsOn)
                .build();
        Task taskFour = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .setCreatedAt(createdAt)
                .setDependsOn(dependsOn)
                .build();
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

    @Test
    public void testTaskStatusListener() throws IOException, InterruptedException {
        String namespace = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        final TestTaskStatusChangeListener taskStatusChangeListener = new TestTaskStatusChangeListener();
        TaskService.getService().registerListener(taskStatusChangeListener);
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskOne);
        List<String> tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(SCHEDULED, taskOne.getStatus());
        finishExecution(taskOne);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertTrue(taskStatusChangeListener.isStatusReceived(taskOne));

        TaskService.getService().deregisterListener(taskStatusChangeListener);
        Task taskTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(job)
                .setNamespace(namespace)
                .setType(TASK_TYPE)
                .build();
        TaskSchedulerService.getService().schedule(taskTwo);
        tasks = TaskSchedulerService.getService().getConsumer().poll(TASK_TYPE);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals(SCHEDULED, taskTwo.getStatus());
        finishExecution(taskTwo);
        sleep(100);
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertFalse(taskStatusChangeListener.isStatusReceived(taskTwo));
    }

    private void finishExecution(Task task) throws JsonProcessingException {
        pushStatusUpdate(task, SUBMITTED);
        pushStatusUpdate(task, RUNNING);
        pushStatusUpdate(task, SUCCESSFUL);
    }

    private void pushStatusUpdate(Task task, Task.Status status) throws JsonProcessingException {
        TaskUpdate taskUpdate = new TaskUpdate();
        taskUpdate.setTaskId(task);
        taskUpdate.setStatus(status);
        taskUpdate.setStatusMessage(task.getStatusMessage());
        TaskSchedulerService.getService().getProducer().send("taskstatus", MAPPER.writeValueAsString(taskUpdate));
    }
}
