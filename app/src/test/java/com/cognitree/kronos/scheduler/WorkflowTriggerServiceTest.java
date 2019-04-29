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

import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.executor.handlers.MockSuccessTaskHandler;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.Scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.TestUtil.createNamespace;
import static com.cognitree.kronos.TestUtil.createWorkflow;
import static com.cognitree.kronos.TestUtil.createWorkflowTrigger;
import static com.cognitree.kronos.TestUtil.scheduleWorkflow;
import static com.cognitree.kronos.TestUtil.waitForTriggerToComplete;

public class WorkflowTriggerServiceTest {
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();
    private static final ExecutorApp EXECUTOR_APP = new ExecutorApp();

    @BeforeClass
    public static void start() throws Exception {
        SCHEDULER_APP.start();
        EXECUTOR_APP.start();
        // wait for the application to initialize itself
        Thread.sleep(100);
    }

    @AfterClass
    public static void stop() {
        SCHEDULER_APP.stop();
        EXECUTOR_APP.stop();
    }

    @Test(expected = ValidationException.class)
    public void testAddWorkflowTriggerWithoutNamespace() throws Exception {
        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        workflowTriggerService.add(workflowTrigger);
        Assert.fail();
    }

    @Test(expected = ValidationException.class)
    public void testAddWorkflowTriggerWithoutWorkflow() throws Exception {
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespace);

        final WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        WorkflowTriggerService.getService().add(workflowTrigger);
        Assert.fail();
    }

    @Test
    public void testAddWorkflowTrigger() throws Exception {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        WorkflowService.getService().add(workflowOne);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowOne.getName(), namespaceOne.getName());
        workflowTriggerService.add(workflowTriggerOne);
        final WorkflowTrigger workflowTriggerOneFromDB = workflowTriggerService.get(workflowTriggerOne);
        Assert.assertNotNull(workflowTriggerOneFromDB);
        Assert.assertEquals(workflowTriggerOne, workflowTriggerOneFromDB);

        Namespace namespaceTwo = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceTwo);

        final Workflow workflowTwo = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceTwo.getName());
        WorkflowService.getService().add(workflowTwo);

        final WorkflowTrigger workflowTriggerTwo = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowTwo.getName(), namespaceTwo.getName());
        workflowTriggerService.add(workflowTriggerTwo);
        final WorkflowTrigger workflowTriggerTwoFromDB = workflowTriggerService.get(workflowTriggerTwo);
        Assert.assertNotNull(workflowTriggerTwoFromDB);
        Assert.assertEquals(workflowTriggerTwo, workflowTriggerTwoFromDB);
    }


    @Test
    public void testGetAllWorkflowTrigger() throws Exception {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        WorkflowService.getService().add(workflowOne);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowOne.getName(), namespaceOne.getName());
        workflowTriggerService.add(workflowTriggerOne);
        final WorkflowTrigger workflowTriggerOneFromDB = workflowTriggerService.get(workflowTriggerOne);
        Assert.assertNotNull(workflowTriggerOneFromDB);
        Assert.assertEquals(workflowTriggerOne, workflowTriggerOneFromDB);


        Namespace namespaceTwo = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceTwo);

        final Workflow workflowTwo = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceTwo.getName());
        WorkflowService.getService().add(workflowTwo);

        final WorkflowTrigger workflowTriggerTwo = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowTwo.getName(), namespaceTwo.getName());
        workflowTriggerService.add(workflowTriggerTwo);
        final WorkflowTrigger workflowTriggerTwoFromDB = workflowTriggerService.get(workflowTriggerTwo);
        Assert.assertNotNull(workflowTriggerTwoFromDB);
        Assert.assertEquals(workflowTriggerTwo, workflowTriggerTwoFromDB);

        Assert.assertEquals(1, workflowTriggerService.get(namespaceOne.getName()).size());
        Assert.assertEquals(1, workflowTriggerService.get(namespaceTwo.getName()).size());
    }

    @Test
    public void testGetAllWorkflowTriggerByWorkflowName() throws Exception {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        WorkflowService.getService().add(workflowOne);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowOne.getName(), namespaceOne.getName());
        workflowTriggerService.add(workflowTriggerOne);
        final WorkflowTrigger workflowTriggerOneFromDB = workflowTriggerService.get(workflowTriggerOne);
        Assert.assertNotNull(workflowTriggerOneFromDB);
        Assert.assertEquals(workflowTriggerOne, workflowTriggerOneFromDB);


        Namespace namespaceTwo = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceTwo);

        final Workflow workflowTwo = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceTwo.getName());
        WorkflowService.getService().add(workflowTwo);

        final WorkflowTrigger workflowTriggerTwo = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowTwo.getName(), namespaceTwo.getName());
        workflowTriggerService.add(workflowTriggerTwo);
        final WorkflowTrigger workflowTriggerTwoFromDB = workflowTriggerService.get(workflowTriggerTwo);
        Assert.assertNotNull(workflowTriggerTwoFromDB);
        Assert.assertEquals(workflowTriggerTwo, workflowTriggerTwoFromDB);

        Assert.assertEquals(1, workflowTriggerService.get(namespaceOne.getName(), workflowOne.getName()).size());
        Assert.assertEquals(1, workflowTriggerService.get(namespaceTwo.getName(), workflowTwo.getName()).size());
    }

    @Test(expected = ValidationException.class)
    public void testReAddWorkflowTrigger() throws Exception {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        WorkflowService.getService().add(workflowOne);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowOne.getName(), namespaceOne.getName());
        workflowTriggerService.add(workflowTriggerOne);
        workflowTriggerService.add(workflowTriggerOne);
        Assert.fail();
    }

    @Test
    public void testDeleteWorkflowTrigger() throws Exception {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        WorkflowService.getService().add(workflowOne);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowOne.getName(), namespaceOne.getName());
        workflowTriggerService.add(workflowTriggerOne);

        workflowTriggerService.delete(workflowTriggerOne);
        final WorkflowTrigger workflowTriggerOneFromDB = workflowTriggerService.get(workflowTriggerOne);
        Assert.assertNull(workflowTriggerOneFromDB);
    }

    @Test
    public void testWorkflowTriggerPropertiesOne() throws Exception {
        HashMap<String, Object> workflowProps = new HashMap<>();
        workflowProps.put("valOne", 1234);
        workflowProps.put("valTwo", "abcd");

        HashMap<String, Object> triggerProps = new HashMap<>();
        triggerProps.put("valOne", 123456);
        triggerProps.put("valTwo", "abcdef");

        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-with-properties.yaml",
                workflowProps, triggerProps);

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(3, workflowTasks.size());
        for (Task workflowTask : workflowTasks) {
            Assert.assertEquals(MockSuccessTaskHandler.CONTEXT, workflowTask.getContext());
            if (workflowTask.getName().equals("taskTwo")) {
                Assert.assertEquals(123456, workflowTask.getProperties().get("keyB"));
            }
            if (workflowTask.getName().equals("taskThree")) {
                Assert.assertEquals("abcdef", workflowTask.getProperties().get("keyB"));
            }
        }
    }

    @Test
    public void testWorkflowTriggerPropertiesTwo() throws Exception {
        HashMap<String, Object> workflowProps = new HashMap<>();
        workflowProps.put("valOne", 1234);
        workflowProps.put("valTwo", "abcd");

        HashMap<String, Object> triggerProps = new HashMap<>();
        triggerProps.put("valOne", 123456);

        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-with-properties.yaml",
                workflowProps, triggerProps);

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(100);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(3, workflowTasks.size());
        for (Task workflowTask : workflowTasks) {
            Assert.assertEquals(workflowTask.getContext(), MockSuccessTaskHandler.CONTEXT);
            if (workflowTask.getName().equals("taskTwo")) {
                Assert.assertEquals(123456, workflowTask.getProperties().get("keyB"));
            }
            if (workflowTask.getName().equals("taskThree")) {
                Assert.assertEquals("abcd", workflowTask.getProperties().get("keyB"));
            }
        }
    }
}
