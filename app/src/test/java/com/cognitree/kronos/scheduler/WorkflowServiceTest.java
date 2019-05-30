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

import com.cognitree.kronos.executor.handlers.MockSuccessTaskHandler;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.cognitree.kronos.TestUtil.createNamespace;
import static com.cognitree.kronos.TestUtil.createWorkflow;
import static com.cognitree.kronos.TestUtil.scheduleWorkflow;
import static com.cognitree.kronos.TestUtil.waitForTriggerToComplete;

public class WorkflowServiceTest extends ServiceTest {

    @Test(expected = ValidationException.class)
    public void testAddWorkflowWithoutNamespace() throws ValidationException, ServiceException, IOException {
        final WorkflowService workflowService = WorkflowService.getService();
        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        workflowService.add(workflow);
        Assert.fail();
    }

    @Test(expected = ValidationException.class)
    public void testAddInValidWorkflowMissingTasks() throws Exception {
        Workflow invalidWorkflow = createWorkflow("workflows/invalid-workflow-missing-tasks-template.yaml",
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        WorkflowService.getService().add(invalidWorkflow);
        Assert.fail();
    }

    @Test(expected = ValidationException.class)
    public void testAddInValidWorkflowDisabledTaskDependency() throws Exception {
        Workflow invalidWorkflow = createWorkflow("workflows/invalid-workflow-disabled-tasks-template.yaml",
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        WorkflowService.getService().add(invalidWorkflow);
        Assert.fail();
    }

    @Test
    public void testAddWorkflow() throws ServiceException, ValidationException, IOException {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final WorkflowService workflowService = WorkflowService.getService();
        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        workflowService.add(workflowOne);

        final Workflow workflowOneFromDB = workflowService.get(workflowOne.getIdentity());
        Assert.assertNotNull(workflowOneFromDB);
        Assert.assertEquals(workflowOne, workflowOneFromDB);

        final Workflow workflowTwo = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        workflowService.add(workflowTwo);

        final Workflow workflowTwoFromDB = workflowService.get(workflowTwo.getIdentity());
        Assert.assertNotNull(workflowTwoFromDB);
        Assert.assertEquals(workflowTwo, workflowTwoFromDB);

        Namespace namespaceTwo = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceTwo);

        final Workflow workflowThree = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceTwo.getName());
        workflowService.add(workflowThree);

        final Workflow workflowThreeFromDB = workflowService.get(workflowThree.getIdentity());
        Assert.assertNotNull(workflowThreeFromDB);
        Assert.assertEquals(workflowThree, workflowThreeFromDB);

        final List<Workflow> namespaceOneWorkflows = workflowService.get(namespaceOne.getName());
        Assert.assertEquals(2, namespaceOneWorkflows.size());
        Assert.assertTrue(namespaceOneWorkflows.contains(workflowOne));
        Assert.assertTrue(namespaceOneWorkflows.contains(workflowTwo));

        final List<Workflow> namespaceTwoWorkflows = workflowService.get(namespaceTwo.getName());
        Assert.assertEquals(1, namespaceTwoWorkflows.size());
        Assert.assertTrue(namespaceTwoWorkflows.contains(workflowThree));
    }

    @Test(expected = ValidationException.class)
    public void testReAddWorkflow() throws ServiceException, ValidationException, IOException {
        final NamespaceService namespaceService = NamespaceService.getService();
        final Namespace namespace = createNamespace(UUID.randomUUID().toString());
        namespaceService.add(namespace);
        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespace.getName());
        WorkflowService.getService().add(workflow);
        WorkflowService.getService().add(workflow);
        Assert.fail();
    }

    @Test
    public void testUpdateWorkflow() throws ServiceException, ValidationException, SchedulerException, IOException {
        final NamespaceService namespaceService = NamespaceService.getService();
        final Namespace namespace = createNamespace(UUID.randomUUID().toString());
        namespaceService.add(namespace);

        final WorkflowService workflowService = WorkflowService.getService();
        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespace.getName());
        workflowService.add(workflow);

        final Workflow updatedWorkflow = createWorkflow("workflows/workflow-template.yaml",
                workflow.getName(), namespace.getName());
        Workflow.WorkflowTask workflowTaskFour = new Workflow.WorkflowTask();
        workflowTaskFour.setName("taskFour");
        workflowTaskFour.setType("typeSuccess");
        updatedWorkflow.getTasks().add(workflowTaskFour);
        workflowService.update(updatedWorkflow);
        final Workflow updatedWorkflowInDB = workflowService.get(updatedWorkflow.getIdentity());
        Assert.assertNotNull(updatedWorkflowInDB);
        Assert.assertEquals(updatedWorkflow, updatedWorkflowInDB);
    }

    @Test
    public void testDeleteWorkflow() throws ServiceException, SchedulerException, ValidationException, IOException {
        final NamespaceService namespaceService = NamespaceService.getService();
        final Namespace namespace = createNamespace(UUID.randomUUID().toString());
        namespaceService.add(namespace);
        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespace.getName());
        WorkflowService.getService().add(workflow);
        final WorkflowService workflowService = WorkflowService.getService();
        workflowService.delete(workflow);
        Assert.assertNull(workflowService.get(workflow.getIdentity()));
    }

    @Test
    public void testTaskWithContextFromWorkflow() throws Exception {
        HashMap<String, Object> workflowProps = new HashMap<>();
        workflowProps.put("valOne", 1234);
        workflowProps.put("valTwo", "abcd");

        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-with-properties.yaml",
                workflowProps, null);

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(1000);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(3, workflowTasks.size());
        for (Task workflowTask : workflowTasks) {
            Assert.assertEquals(workflowTask.getContext(), MockSuccessTaskHandler.CONTEXT);
            if (workflowTask.getName().equals("taskTwo")) {
                Assert.assertEquals(1234, workflowTask.getProperties().get("keyB"));
            }
            if (workflowTask.getName().equals("taskThree")) {
                Assert.assertEquals("abcd", workflowTask.getProperties().get("keyB"));
                Assert.assertEquals("abcd", ((Map<String, Object>) workflowTask.getProperties().get("keyC")).get("keyA"));
                Assert.assertEquals("valB", ((Map<String, Object>) workflowTask.getProperties().get("keyC")).get("keyB"));
            }
        }
    }


    @Test(expected = ValidationException.class)
    public void testDuplicatePolicyOfSameType() throws Exception {
        scheduleWorkflow("workflows/workflow-template-with-duplicate-policy.yaml",
                null, null);
        Assert.fail();
    }

    @Test(expected = ValidationException.class)
    public void testMissingWorkflowPropertiesShouldFail() throws Exception {
        HashMap<String, Object> workflowProps = new HashMap<>();
        workflowProps.put("valOne", 1234);
        scheduleWorkflow("workflows/workflow-template-with-properties.yaml", workflowProps, null);
        Assert.fail();
    }
}
