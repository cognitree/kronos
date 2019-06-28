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

package com.cognitree.kronos.executor;

import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.executor.handlers.TestTaskHandler;
import com.cognitree.kronos.executor.handlers.TypeATaskHandler;
import com.cognitree.kronos.executor.handlers.TypeBTaskHandler;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.model.TaskStatusUpdate;
import com.cognitree.kronos.queue.QueueService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.queue.QueueService.EXECUTOR_QUEUE;

public class TaskExecutorServiceTest {

    private static final ExecutorApp EXECUTOR_APP = new ExecutorApp();
    private static final String TASK_TYPE_TEST = "test";
    private static final String TASK_TYPE_B = "typeB";
    private static final String TASK_TYPE_A = "typeA";

    @BeforeClass
    public static void start() throws Exception {
        EXECUTOR_APP.start();
    }

    @AfterClass
    public static void stop() {
        EXECUTOR_APP.stop();
    }

    @Test
    public void testTaskExecution() throws Exception {
        final HashMap<TaskId, Task> tasksMap = new HashMap<>();
        String namespace = UUID.randomUUID().toString();
        String jobId = UUID.randomUUID().toString();
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_TEST)
                .setStatus(SCHEDULED)
                .build();
        tasksMap.put(taskOne, taskOne);
        QueueService.getService(EXECUTOR_QUEUE).send(taskOne);
        Thread.sleep(1000);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(RUNNING, taskOne.getStatus());
        TestTaskHandler.finishExecution(taskOne.getName());
        Thread.sleep(1000);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
    }

    @Test
    public void testTaskExecutionNegative() throws Exception {
        final HashMap<TaskId, Task> tasksMap = new HashMap<>();
        String namespace = UUID.randomUUID().toString();
        String jobId = UUID.randomUUID().toString();
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_B)
                .setStatus(SCHEDULED)
                .build();
        tasksMap.put(taskOne, taskOne);
        QueueService.getService(EXECUTOR_QUEUE).send(taskOne);
        Thread.sleep(1000);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(FAILED, taskOne.getStatus());
        Assert.assertEquals("error handling task", taskOne.getStatusMessage());
    }

    @Test
    public void testMaxParallelTask() throws Exception {
        final HashMap<TaskId, Task> tasksMap = new HashMap<>();
        String namespace = UUID.randomUUID().toString();
        String jobId = UUID.randomUUID().toString();
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_TEST)
                .setStatus(SCHEDULED)
                .build();
        tasksMap.put(taskOne, taskOne);
        QueueService.getService(EXECUTOR_QUEUE).send(taskOne);

        Task taskTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_TEST)
                .setStatus(SCHEDULED)
                .build();
        tasksMap.put(taskTwo, taskTwo);
        QueueService.getService(EXECUTOR_QUEUE).send(taskTwo);

        Task taskThree = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_TEST)
                .setStatus(SCHEDULED)
                .build();
        tasksMap.put(taskThree, taskThree);
        QueueService.getService(EXECUTOR_QUEUE).send(taskThree);

        Task taskFour = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_TEST)
                .setStatus(SCHEDULED)
                .build();
        tasksMap.put(taskFour, taskFour);
        QueueService.getService(EXECUTOR_QUEUE).send(taskFour);

        Task taskFive = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_TEST)
                .setStatus(SCHEDULED)
                .build();
        tasksMap.put(taskFive, taskFive);
        QueueService.getService(EXECUTOR_QUEUE).send(taskFive);

        Thread.sleep(1000);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(RUNNING, taskOne.getStatus());
        Assert.assertEquals(RUNNING, taskTwo.getStatus());
        Assert.assertEquals(RUNNING, taskThree.getStatus());
        Assert.assertEquals(RUNNING, taskFour.getStatus());
        Assert.assertEquals(SCHEDULED, taskFive.getStatus());
        TestTaskHandler.finishExecution(taskOne.getName());
        Thread.sleep(1000);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(RUNNING, taskFive.getStatus());
        TestTaskHandler.finishExecution(taskTwo.getName());
        TestTaskHandler.finishExecution(taskThree.getName());
        TestTaskHandler.finishExecution(taskFour.getName());
        TestTaskHandler.finishExecution(taskFive.getName());
        Thread.sleep(1000);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskFour.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskFive.getStatus());
    }

    private void consumeTaskStatus(HashMap<TaskId, Task> tasksMap) throws ServiceException {
        final List<TaskStatusUpdate> taskStatusUpdates = QueueService.getService(EXECUTOR_QUEUE).consumeTaskStatusUpdates();
        taskStatusUpdates.forEach(taskUpdate -> {
            final Task task = tasksMap.get(taskUpdate.getTaskId());
            task.setStatus(taskUpdate.getStatus());
            task.setStatusMessage(taskUpdate.getStatusMessage());
        });
    }

    @Test
    public void testTaskToHandlerMapping() throws Exception {
        String namespace = UUID.randomUUID().toString();
        String jobId = UUID.randomUUID().toString();
        Task taskOne = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_A)
                .setStatus(SCHEDULED)
                .build();
        QueueService.getService(EXECUTOR_QUEUE).send(taskOne);
        Task taskTwo = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_B)
                .setStatus(SCHEDULED)
                .build();
        QueueService.getService(EXECUTOR_QUEUE).send(taskTwo);
        Task taskThree = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_A)
                .setStatus(SCHEDULED)
                .build();
        QueueService.getService(EXECUTOR_QUEUE).send(taskThree);
        Task taskFour = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_A)
                .setStatus(SCHEDULED)
                .build();
        QueueService.getService(EXECUTOR_QUEUE).send(taskFour);
        Task taskFive = MockTaskBuilder.getTaskBuilder()
                .setJob(jobId)
                .setNamespace(namespace)
                .setType(TASK_TYPE_B)
                .setStatus(SCHEDULED)
                .build();
        QueueService.getService(EXECUTOR_QUEUE).send(taskFive);
        Thread.sleep(1000);
        Assert.assertTrue(TypeATaskHandler.isHandled(taskOne.getName()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskOne.getName()));
        Assert.assertTrue(TypeBTaskHandler.isHandled(taskTwo.getName()));
        Assert.assertFalse(TypeATaskHandler.isHandled(taskTwo.getName()));
        Assert.assertTrue(TypeATaskHandler.isHandled(taskThree.getName()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskThree.getName()));
        Assert.assertTrue(TypeATaskHandler.isHandled(taskFour.getName()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskFour.getName()));
        Assert.assertTrue(TypeBTaskHandler.isHandled(taskFive.getName()));
        Assert.assertFalse(TypeATaskHandler.isHandled(taskFive.getName()));
    }
}

