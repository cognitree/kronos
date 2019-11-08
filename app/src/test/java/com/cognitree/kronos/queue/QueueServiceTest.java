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

package com.cognitree.kronos.queue;

import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.model.ControlMessage;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskStatusUpdate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;

public class QueueServiceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final String SERVICE_NAME = "queue-service";
    private static final String TASK_TYPE_A = "typeA";
    private static final String TASK_TYPE_B = "typeB";
    private static final int WAIT_FOR_NEXT_POLL = 2000;

    private static QueueService QUEUE_SERVICE;

    @BeforeClass
    public static void init() throws IOException, ServiceException {
        final InputStream queueConfigAsStream =
                QueueServiceTest.class.getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);
        QUEUE_SERVICE = new QueueService(queueConfig, SERVICE_NAME);
        QUEUE_SERVICE.init();
        QUEUE_SERVICE.start();
        // initial call so that the topics are created
        QUEUE_SERVICE.consumeTasks(TASK_TYPE_A, 1);
        QUEUE_SERVICE.consumeTasks(TASK_TYPE_B, 1);
        QUEUE_SERVICE.consumeTaskStatusUpdates();
        QUEUE_SERVICE.consumeControlMessages();
    }

    @AfterClass
    public static void destroy() {
        QUEUE_SERVICE.stop();
        QUEUE_SERVICE.destroy();
    }

    /**
     * Test unordered messages
     *
     * @throws ServiceException
     */
    @Test
    public void testSendAndConsumeTask() throws ServiceException {
        Task taskAOne = createTask(TASK_TYPE_A);
        QUEUE_SERVICE.send(taskAOne);
        Task taskATwo = createTask(TASK_TYPE_A);
        QUEUE_SERVICE.send(taskATwo);
        Task taskAThree = createTask(TASK_TYPE_A);
        QUEUE_SERVICE.send(taskAThree);

        Task taskBOne = createTask(TASK_TYPE_B);
        QUEUE_SERVICE.send(taskBOne);
        Task taskBTwo = createTask(TASK_TYPE_B);
        QUEUE_SERVICE.send(taskBTwo);
        Task taskBThree = createTask(TASK_TYPE_B);
        QUEUE_SERVICE.send(taskBThree);

        ArrayList<Task> tasksB = new ArrayList<>();
        tasksB.add(taskBOne);
        tasksB.add(taskBTwo);
        tasksB.add(taskBThree);

        List<Task> tasksBConsumed = getTasks(TASK_TYPE_B, 3);
        Assert.assertTrue("Records sent " + tasksB + " and records consumed" + tasksBConsumed + " do not match",
                tasksBConsumed.size() == tasksB.size() && tasksBConsumed.containsAll(tasksB));

        ArrayList<Task> tasksA = new ArrayList<>();
        tasksA.add(taskAOne);
        tasksA.add(taskATwo);
        tasksA.add(taskAThree);

        List<Task> tasksAConsumed = getTasks(TASK_TYPE_A, 3);
        Assert.assertTrue("Records sent " + tasksA + " and records consumed" + tasksAConsumed + " do not match",
                tasksAConsumed.size() == tasksA.size() && tasksAConsumed.containsAll(tasksA));
    }

    /**
     * Test ordered messages
     *
     * @throws ServiceException
     */
    @Test
    public void testSendAndConsumeTaskStatusUpdates() throws ServiceException {
        Task task = createTask(TASK_TYPE_A);
        TaskStatusUpdate taskStatusUpdateOne = createTaskStatusUpdate(task, RUNNING);
        QUEUE_SERVICE.send(taskStatusUpdateOne);
        TaskStatusUpdate taskStatusUpdateTwo = createTaskStatusUpdate(task, FAILED);
        QUEUE_SERVICE.send(taskStatusUpdateTwo);
        TaskStatusUpdate taskStatusUpdateThree = createTaskStatusUpdate(task, SUCCESSFUL);
        QUEUE_SERVICE.send(taskStatusUpdateThree);

        List<TaskStatusUpdate> taskStatusUpdates = getTaskStatusUpdate();
        Assert.assertFalse(taskStatusUpdates.isEmpty());
        Assert.assertEquals(taskStatusUpdateOne, taskStatusUpdates.get(0));
        Assert.assertEquals(taskStatusUpdateTwo, taskStatusUpdates.get(1));
        Assert.assertEquals(taskStatusUpdateThree, taskStatusUpdates.get(2));
    }

    @Test
    public void testSendAndConsumeControlMessageUpdates() throws ServiceException {
        Task task = createTask(TASK_TYPE_A);
        ControlMessage controlMessageOne = createControlMessages(task, Task.Action.ABORT);
        QUEUE_SERVICE.send(controlMessageOne);
        ControlMessage controlMessageTwo = createControlMessages(task, Task.Action.ABORT);
        QUEUE_SERVICE.send(controlMessageTwo);
        ControlMessage controlMessageThree = createControlMessages(task, Task.Action.TIME_OUT);
        QUEUE_SERVICE.send(controlMessageThree);

        ArrayList<ControlMessage> controlMessages = new ArrayList<>();
        controlMessages.add(controlMessageOne);
        controlMessages.add(controlMessageTwo);
        controlMessages.add(controlMessageThree);

        List<ControlMessage> controlMessagesConsumed = getControlMessages();
        Assert.assertFalse(controlMessagesConsumed.isEmpty());

        Assert.assertTrue("Records sent " + controlMessages + " and records consumed" + controlMessagesConsumed + " do not match",
                controlMessagesConsumed.size() == controlMessages.size() && controlMessagesConsumed.containsAll(controlMessages));
    }

    private List<ControlMessage> getControlMessages() throws ServiceException {
        int count = 10;
        while (count > 0) {
            List<ControlMessage> controlMessages = QUEUE_SERVICE.consumeControlMessages();
            if (!controlMessages.isEmpty()) {
                return controlMessages;
            }
            try {
                count--;
                Thread.sleep(WAIT_FOR_NEXT_POLL);
            } catch (InterruptedException e) {
                //
            }
        }
        return Collections.emptyList();
    }


    private List<TaskStatusUpdate> getTaskStatusUpdate() throws ServiceException {
        int count = 10;
        while (count > 0) {
            List<TaskStatusUpdate> taskStatusUpdates = QUEUE_SERVICE.consumeTaskStatusUpdates();
            if (!taskStatusUpdates.isEmpty()) {
                return taskStatusUpdates;
            }
            try {
                count--;
                Thread.sleep(WAIT_FOR_NEXT_POLL);
            } catch (InterruptedException e) {
                //
            }
        }
        return Collections.emptyList();
    }

    private List<Task> getTasks(String taskType, int size) throws ServiceException {
        int count = 10;
        while (count > 0) {
            List<Task> tasks = QUEUE_SERVICE.consumeTasks(taskType, size);
            if (!tasks.isEmpty()) {
                return tasks;
            }
            try {
                count--;
                Thread.sleep(WAIT_FOR_NEXT_POLL);
            } catch (InterruptedException e) {
                //
            }
        }
        return Collections.emptyList();
    }

    private Task createTask(String type) {
        final Task task = new Task();
        task.setName(UUID.randomUUID().toString());
        task.setWorkflow(UUID.randomUUID().toString());
        task.setJob(UUID.randomUUID().toString());
        task.setType(type);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("A", "A");
        properties.put("B", "B");
        task.setProperties(properties);
        task.setCreatedAt(System.currentTimeMillis());
        return task;
    }

    private TaskStatusUpdate createTaskStatusUpdate(Task task, Task.Status status) {
        final TaskStatusUpdate statusUpdate = new TaskStatusUpdate();
        statusUpdate.setTaskId(task);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("A", "A");
        properties.put("B", "B");
        statusUpdate.setContext(properties);
        statusUpdate.setStatus(status);
        return statusUpdate;
    }

    private ControlMessage createControlMessages(Task task, Task.Action action) {
        final ControlMessage controlMessage = new ControlMessage();
        controlMessage.setTask(task);
        controlMessage.setAction(action);
        return controlMessage;
    }

}
