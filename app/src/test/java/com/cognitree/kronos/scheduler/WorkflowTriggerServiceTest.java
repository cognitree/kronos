package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ApplicationTest;
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowTrigger;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class WorkflowTriggerServiceTest extends ApplicationTest {

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

        Assert.assertEquals(1, workflowTriggerService.get(workflowOne.getName(), namespaceOne.getName()).size());
        Assert.assertEquals(1, workflowTriggerService.get(workflowTwo.getName(), namespaceTwo.getName()).size());
    }

    @Test(expected = ServiceException.class)
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
    public void testUpdateWorkflowTrigger() throws Exception {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        WorkflowService.getService().add(workflowOne);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowOne.getName(), namespaceOne.getName());
        workflowTriggerService.add(workflowTriggerOne);

        final WorkflowTrigger updatedWorkflowTriggerOne = createWorkflowTrigger(workflowTriggerOne.getName(),
                workflowOne.getName(), namespaceOne.getName()
        );
        workflowTriggerService.update(updatedWorkflowTriggerOne);
        final WorkflowTrigger workflowTriggerOneFromDB = workflowTriggerService.get(updatedWorkflowTriggerOne);
        Assert.assertEquals(updatedWorkflowTriggerOne, workflowTriggerOneFromDB);
    }

    @Test(expected = ServiceException.class)
    public void testUpdateWorkflowTriggerInvalid() throws Exception {
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespaceOne);

        final Workflow workflowOne = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespaceOne.getName());
        WorkflowService.getService().add(workflowOne);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflowOne.getName(), namespaceOne.getName());
        workflowTriggerService.update(workflowTriggerOne);
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
}
