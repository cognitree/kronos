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

import com.cognitree.kronos.MockTaskBuilder;
import com.cognitree.kronos.executor.handlers.TestTaskHandler;
import com.cognitree.kronos.executor.handlers.TypeATaskHandler;
import com.cognitree.kronos.executor.handlers.TypeBTaskHandler;
import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static com.cognitree.kronos.model.Task.Status.*;
import static java.lang.Thread.sleep;

public class TaskExecutorServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ExecutorApp EXECUTOR_APP = new ExecutorApp();

    @BeforeClass
    public static void init() throws Exception {
        EXECUTOR_APP.start();
    }

    @AfterClass
    public static void stop() {
        EXECUTOR_APP.stop();
    }

    @Test
    public void testTaskExecution() throws JsonProcessingException, InterruptedException {
        final HashMap<String, Task> tasksMap = new HashMap<>();
        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType("test").setStatus(SCHEDULED).build();
        tasksMap.put(taskOne.getId(), taskOne);
        TaskExecutionService.getService().getProducer().send(taskOne.getType(), MAPPER.writeValueAsString(taskOne));
        sleep(100);
        consumeTaskStatus(tasksMap);
        // depending on the number of available cores task picked for execution
        // will be in one of the two state RUNNING or SUBMITTED
        Assert.assertTrue(taskOne.getStatus().equals(RUNNING) || taskOne.getStatus().equals(SUBMITTED));
        TestTaskHandler.finishExecution(taskOne.getId());
        sleep(100);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
    }

    @Test
    public void testMaxParallelTask() throws InterruptedException, JsonProcessingException {
        final HashMap<String, Task> tasksMap = new HashMap<>();

        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType("test").setStatus(SCHEDULED).build();
        tasksMap.put(taskOne.getId(), taskOne);
        TaskExecutionService.getService().getProducer().send(taskOne.getType(), MAPPER.writeValueAsString(taskOne));

        Task taskTwo = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setType("test").setStatus(SCHEDULED).build();
        tasksMap.put(taskTwo.getId(), taskTwo);
        TaskExecutionService.getService().getProducer().send(taskTwo.getType(), MAPPER.writeValueAsString(taskTwo));

        Task taskThree = MockTaskBuilder.getTaskBuilder().setName("taskThree").setType("test").setStatus(SCHEDULED).build();
        tasksMap.put(taskThree.getId(), taskThree);
        TaskExecutionService.getService().getProducer().send(taskThree.getType(), MAPPER.writeValueAsString(taskThree));

        Task taskFour = MockTaskBuilder.getTaskBuilder().setName("taskFour").setType("test").setStatus(SCHEDULED).build();
        tasksMap.put(taskFour.getId(), taskFour);
        TaskExecutionService.getService().getProducer().send(taskFour.getType(), MAPPER.writeValueAsString(taskFour));

        Task taskFive = MockTaskBuilder.getTaskBuilder().setName("taskFive").setType("test").setStatus(SCHEDULED).build();
        tasksMap.put(taskFive.getId(), taskFive);
        TaskExecutionService.getService().getProducer().send(taskFive.getType(), MAPPER.writeValueAsString(taskFive));

        sleep(100);
        consumeTaskStatus(tasksMap);
        // depending on the number of available cores task picked for execution
        // will be in one of the two state RUNNING or SUBMITTED
        Assert.assertTrue(taskOne.getStatus().equals(RUNNING) || taskOne.getStatus().equals(SUBMITTED));
        Assert.assertTrue(taskTwo.getStatus().equals(RUNNING) || taskTwo.getStatus().equals(SUBMITTED));
        Assert.assertTrue(taskThree.getStatus().equals(RUNNING) || taskThree.getStatus().equals(SUBMITTED));
        Assert.assertTrue(taskFour.getStatus().equals(RUNNING) || taskFour.getStatus().equals(SUBMITTED));
        Assert.assertEquals(SCHEDULED, taskFive.getStatus());
        TestTaskHandler.finishExecution(taskOne.getId());
        sleep(100);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertTrue(taskFive.getStatus().equals(RUNNING) || taskFive.getStatus().equals(SUBMITTED));
        TestTaskHandler.finishExecution(taskTwo.getId());
        TestTaskHandler.finishExecution(taskThree.getId());
        TestTaskHandler.finishExecution(taskFour.getId());
        TestTaskHandler.finishExecution(taskFive.getId());
        sleep(100);
        consumeTaskStatus(tasksMap);
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskFour.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskFive.getStatus());
    }

    private void consumeTaskStatus(HashMap<String, Task> tasksMap) {
        final List<String> tasksStatus = TaskExecutionService.getService().getConsumer().poll("taskstatus");
        tasksStatus.forEach(taskStatus -> {
            try {
                final Task.TaskUpdate taskUpdate = MAPPER.readValue(taskStatus, Task.TaskUpdate.class);
                final MutableTask task = (MutableTask) tasksMap.get(taskUpdate.getTaskId());
                task.setStatus(taskUpdate.getStatus());
                task.setStatusMessage(taskUpdate.getStatusMessage());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testTaskToHandlerMapping() throws InterruptedException, JsonProcessingException {
        Task taskOne = MockTaskBuilder.getTaskBuilder().setName("taskOne").setType("typeA").build();
        TaskExecutionService.getService().getProducer().send(taskOne.getType(), MAPPER.writeValueAsString(taskOne));
        Task taskTwo = MockTaskBuilder.getTaskBuilder().setName("taskTwo").setType("typeB").build();
        TaskExecutionService.getService().getProducer().send(taskTwo.getType(), MAPPER.writeValueAsString(taskTwo));
        Task taskThree = MockTaskBuilder.getTaskBuilder().setName("taskThree").setType("typeA").build();
        TaskExecutionService.getService().getProducer().send(taskThree.getType(), MAPPER.writeValueAsString(taskThree));
        Task taskFour = MockTaskBuilder.getTaskBuilder().setName("taskFour").setType("typeA").build();
        TaskExecutionService.getService().getProducer().send(taskFour.getType(), MAPPER.writeValueAsString(taskFour));
        Task taskFive = MockTaskBuilder.getTaskBuilder().setName("taskFive").setType("typeB").build();
        TaskExecutionService.getService().getProducer().send(taskFive.getType(), MAPPER.writeValueAsString(taskFive));
        sleep(100);
        Assert.assertTrue(TypeATaskHandler.isHandled(taskOne.getId()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskOne.getId()));
        Assert.assertTrue(TypeBTaskHandler.isHandled(taskTwo.getId()));
        Assert.assertFalse(TypeATaskHandler.isHandled(taskTwo.getId()));
        Assert.assertTrue(TypeATaskHandler.isHandled(taskThree.getId()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskThree.getId()));
        Assert.assertTrue(TypeATaskHandler.isHandled(taskFour.getId()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskFour.getId()));
        Assert.assertTrue(TypeBTaskHandler.isHandled(taskFive.getId()));
        Assert.assertFalse(TypeATaskHandler.isHandled(taskFive.getId()));
    }
}

