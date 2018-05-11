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

package com.cognitree.tasks.scheduler;

import com.cognitree.tasks.ApplicationConfig;
import com.cognitree.tasks.ServiceProvider;
import com.cognitree.tasks.executor.TaskExecutionService;
import com.cognitree.tasks.executor.handlers.TestTaskHandler;
import com.cognitree.tasks.model.Task;
import com.cognitree.tasks.model.TaskDependencyInfo;
import com.cognitree.tasks.model.TaskStatus;
import com.cognitree.tasks.queue.consumer.Consumer;
import com.cognitree.tasks.queue.consumer.ConsumerConfig;
import com.cognitree.tasks.queue.producer.Producer;
import com.cognitree.tasks.queue.producer.ProducerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.*;

import java.io.InputStream;
import java.util.*;

import static com.cognitree.tasks.model.Task.Status.*;
import static com.cognitree.tasks.model.TaskDependencyInfo.Mode.all;
import static java.util.concurrent.TimeUnit.DAYS;

public class ApplicationTest {

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void initTaskProviderService() throws Exception {
        InputStream appResourceStream = ApplicationTest.class.getClassLoader().getResourceAsStream("app.yaml");
        ApplicationConfig applicationConfig = new ObjectMapper(new YAMLFactory()).readValue(appResourceStream, ApplicationConfig.class);

        final ProducerConfig taskProducerConfig = applicationConfig.getTaskProducerConfig();
        Producer<Task> taskProducer = (Producer<Task>) Class.forName(taskProducerConfig.getProducerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(taskProducerConfig.getConfig());

        final ConsumerConfig statusConsumerConfig = applicationConfig.getTaskStatusConsumerConfig();
        Consumer<TaskStatus> statusConsumer = (Consumer<TaskStatus>) Class.forName(statusConsumerConfig.getConsumerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(statusConsumerConfig.getConfig());

        TaskProviderService taskProviderService =
                new TaskProviderService(taskProducer, statusConsumer, applicationConfig.getHandlerConfig(),
                        applicationConfig.getTimeoutPolicyConfig(), applicationConfig.getStoreProvider(),
                        applicationConfig.getTaskPurgeInterval());
        ServiceProvider.registerService(taskProviderService);
        taskProviderService.init();
        taskProviderService.start();

        final ProducerConfig statusProducerConfig = applicationConfig.getTaskStatusProducerConfig();
        Producer<TaskStatus> statusProducer = (Producer<TaskStatus>) Class.forName(statusProducerConfig.getProducerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(statusProducerConfig.getConfig());

        final ConsumerConfig taskConsumerConfig = applicationConfig.getTaskConsumerConfig();
        Consumer<Task> taskConsumer = (Consumer<Task>) Class.forName(taskConsumerConfig.getConsumerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(taskConsumerConfig.getConfig());

        TaskExecutionService taskExecutionService =
                new TaskExecutionService(taskConsumer, statusProducer, applicationConfig.getHandlerConfig());
        ServiceProvider.registerService(taskExecutionService);
        taskExecutionService.init();
        taskExecutionService.start();
    }

    @AfterClass
    public static void stop() {
        ServiceProvider.getTaskExecutionService().stop();
        ServiceProvider.getTaskProviderService().stop();
    }

    @Before
    public void initialize() {
        // reinit will clear all the tasks from task provider
        ServiceProvider.getTaskProviderService().getTaskProvider().reinit();
    }

    @Test
    public void testAddIndependentJob() {
        Task taskOne = createTask("taskOne", System.currentTimeMillis(),
                null, new HashMap<>());
        ServiceProvider.getTaskProviderService().add(taskOne);
        waitForTaskToFinishExecution();
        final TaskProvider taskGraph = ServiceProvider.getTaskProviderService().getTaskProvider();
        Assert.assertEquals(1, taskGraph.size());
        Assert.assertTrue(TestTaskHandler.isExecuted(taskOne.getId()));
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());

        Task taskTwo = createTask("taskTwo", System.currentTimeMillis(),
                null, new HashMap<>());
        ServiceProvider.getTaskProviderService().add(taskTwo);
        waitForTaskToFinishExecution();
        Assert.assertEquals(2, taskGraph.size());
        Assert.assertTrue(TestTaskHandler.isExecuted(taskTwo.getId()));
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());

        Task taskThree = createTask("taskThree", System.currentTimeMillis(),
                null, new HashMap<>());
        ServiceProvider.getTaskProviderService().add(taskThree);
        waitForTaskToFinishExecution();
        Assert.assertEquals(3, taskGraph.size());
        Assert.assertTrue(TestTaskHandler.isExecuted(taskThree.getId()));
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void testDuplicateTasks() {
        Task taskOne = createTask("taskOne", System.currentTimeMillis(), null, new HashMap<>());
        ServiceProvider.getTaskProviderService().add(taskOne);
        final TaskProvider taskGraph = ServiceProvider.getTaskProviderService().getTaskProvider();
        Assert.assertEquals(1, taskGraph.size());
        Task taskTwo = createTask("taskOne", System.currentTimeMillis(), null, new HashMap<>());
        taskTwo.setId(taskOne.getId());
        ServiceProvider.getTaskProviderService().add(taskTwo);
        waitForTaskToFinishExecution();
        Assert.assertEquals(1, taskGraph.size());
        Assert.assertTrue(TestTaskHandler.isExecuted(taskOne.getId()));
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
    }

    @Test
    public void testCleanup() {

        final long createdAt = System.currentTimeMillis() - DAYS.toMillis(2);
        Task dummyIndependentTask = createTask("dummyIndependentTask",
                createdAt, null, new HashMap<>());
        ServiceProvider.getTaskProviderService().add(dummyIndependentTask);
        waitForTaskToFinishExecution();

        final Map<String, Object> taskProperties = Collections.singletonMap("status", RUNNING);
        Task taskOne = createTask("taskOne", createdAt, null, taskProperties);
        Task taskTwo = createTask("taskTwo", createdAt, null, taskProperties);
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all));
        Task taskThree = createTask("taskThree", createdAt + 5, dependencyInfos, taskProperties);
        Task taskFour = createTask("taskFour", createdAt + 5, dependencyInfos, taskProperties);
        ServiceProvider.getTaskProviderService().add(taskOne);
        waitForTaskToFinishExecution();
        ServiceProvider.getTaskProviderService().add(taskTwo);
        waitForTaskToFinishExecution();
        ServiceProvider.getTaskProviderService().add(taskThree);
        waitForTaskToFinishExecution();
        ServiceProvider.getTaskProviderService().add(taskFour);
        waitForTaskToFinishExecution();

        final TaskProvider taskProvider = ServiceProvider.getTaskProviderService().getTaskProvider();
        Assert.assertEquals(5, taskProvider.size());
        ServiceProvider.getTaskProviderService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());

        taskProvider.updateTask(taskOne.getId(), taskOne.getGroup(), SUCCESSFUL, null, null);
        ServiceProvider.getTaskProviderService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());

        taskProvider.updateTask(taskTwo.getId(), taskTwo.getGroup(), SUCCESSFUL, null, null);
        ServiceProvider.getTaskProviderService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());

        taskProvider.updateTask(taskThree.getId(), taskThree.getGroup(), SUBMITTED, null, null);
        ServiceProvider.getTaskProviderService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());

        taskProvider.updateTask(taskThree.getId(), taskThree.getGroup(), SUCCESSFUL, null, null);
        ServiceProvider.getTaskProviderService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());

        taskProvider.updateTask(taskFour.getId(), taskFour.getGroup(), SUBMITTED, null, null);
        ServiceProvider.getTaskProviderService().deleteStaleTasks();
        Assert.assertEquals(4, taskProvider.size());

        taskProvider.updateTask(taskFour.getId(), taskFour.getGroup(), SUCCESSFUL, null, null);
        ServiceProvider.getTaskProviderService().deleteStaleTasks();
        Assert.assertEquals(0, taskProvider.size());
    }

    @Test
    public void addTaskWithDependency() {
        final long scheduledAt = System.currentTimeMillis();
        Task taskOne = createTask("taskOne", scheduledAt, null, new HashMap<>());
        Task taskTwo = createTask("taskTwo", scheduledAt, null, new HashMap<>());
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all));
        Task taskThree = createTask("taskThree", scheduledAt + 5, dependencyInfos, new HashMap<>());
        ServiceProvider.getTaskProviderService().add(taskOne);
        ServiceProvider.getTaskProviderService().add(taskTwo);
        waitForTaskToFinishExecution();
        ServiceProvider.getTaskProviderService().add(taskThree);
        waitForTaskToFinishExecution();
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
    }

    @Test
    public void addTaskWithMissingDependency() {
        final long scheduledAt = System.currentTimeMillis();
        Task taskOne = createTask("taskOne", scheduledAt, null, new HashMap<>());
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all));
        Task taskThree = createTask("taskThree", scheduledAt + 5, dependencyInfos, new HashMap<>());

        ServiceProvider.getTaskProviderService().add(taskOne);
        ServiceProvider.getTaskProviderService().add(taskThree);
        // wait for few seconds for the task
        waitForTaskToFinishExecution();
        final TaskProvider taskGraph = ServiceProvider.getTaskProviderService().getTaskProvider();
        Assert.assertEquals(2, taskGraph.size());
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(FAILED, taskThree.getStatus());
    }

    @Test
    public void testTaskDependency() {
        final long scheduledAt = System.currentTimeMillis();
        Task taskOne = createTask("taskOne", scheduledAt, null, new HashMap<>());
        Task taskTwo = createTask("taskTwo", scheduledAt, null, new HashMap<>());
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all));
        Task taskThree = createTask("taskThree", scheduledAt + 5, dependencyInfos, new HashMap<>());

        ServiceProvider.getTaskProviderService().add(taskOne);
        final TaskProvider taskGraph = ServiceProvider.getTaskProviderService().getTaskProvider();
        Assert.assertTrue(taskGraph.isDependencyResolved(taskOne));
        ServiceProvider.getTaskProviderService().add(taskTwo);
        Assert.assertTrue(taskGraph.isDependencyResolved((taskTwo)));
        waitForTaskToFinishExecution();
        ServiceProvider.getTaskProviderService().add(taskThree);
        Assert.assertTrue(taskGraph.isDependencyResolved(taskThree));
        waitForTaskToFinishExecution();

        // manually update task status to FAILED and check dependency resolved should fail now
        taskOne.setStatus(FAILED);
        Assert.assertFalse(taskGraph.isDependencyResolved(taskThree));
    }

    @Test
    public void testTaskFailure() {
        final long scheduledAt = System.currentTimeMillis();
        Task taskOne = createTask("taskOne", scheduledAt, null, new HashMap<>());
        final Map<String, Object> taskProperties = Collections.singletonMap("status", FAILED);
        Task taskTwo = createTask("taskTwo", scheduledAt, null,
                taskProperties);
        List<TaskDependencyInfo> dependencyInfos = new ArrayList<>();
        dependencyInfos.add(prepareDependencyInfo("taskOne", all));
        dependencyInfos.add(prepareDependencyInfo("taskTwo", all));
        Task taskThree = createTask("taskThree", scheduledAt + 5, dependencyInfos, new HashMap<>());

        ServiceProvider.getTaskProviderService().add(taskOne);
        ServiceProvider.getTaskProviderService().add(taskTwo);
        waitForTaskToFinishExecution();
        Assert.assertEquals(FAILED, taskTwo.getStatus());
        ServiceProvider.getTaskProviderService().add(taskThree);
        waitForTaskToFinishExecution();
        Assert.assertEquals(FAILED, taskThree.getStatus());
    }

    private Task createTask(String name, long timestamp, List<TaskDependencyInfo> dependencyInfoList,
                            Map<String, Object> properties) {
        Task task = new Task();
        task.setGroup("DEFAULT");
        task.setType("test");
        task.setName(name);
        task.setId(UUID.randomUUID().toString());
        task.setDependsOn(dependencyInfoList);
        task.setProperties(properties);
        task.setCreatedAt(timestamp);
        return task;
    }

    private TaskDependencyInfo prepareDependencyInfo(String taskName, TaskDependencyInfo.Mode mode) {
        TaskDependencyInfo dependencyInfo1 = new TaskDependencyInfo();
        dependencyInfo1.setName(taskName);
        dependencyInfo1.setDuration("3d");
        dependencyInfo1.setMode(mode);
        return dependencyInfo1;
    }


    private void waitForTaskToFinishExecution() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
