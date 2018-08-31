package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ApplicationTest;
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Workflow;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class WorkflowServiceTest extends ApplicationTest {

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

    @Test(expected = ServiceException.class)
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
}
