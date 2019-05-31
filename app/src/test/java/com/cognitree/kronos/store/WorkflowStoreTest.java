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

package com.cognitree.kronos.store;

import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class WorkflowStoreTest extends StoreTest {

    @Before
    public void setup() throws Exception {
        // initialize workflow store with some workflows
        for (int i = 0; i < 5; i++) {
            ArrayList<Workflow.WorkflowTask> workflowOneTasks = new ArrayList<>();
            workflowOneTasks.add(createWorkflowTask("taskOne", "typeA"));
            workflowOneTasks.add(createWorkflowTask("taskTwo", "typeB"));
            workflowOneTasks.add(createWorkflowTask("taskThree", "typeC"));
            createWorkflow(UUID.randomUUID().toString(), UUID.randomUUID().toString(), workflowOneTasks);
        }
    }

    @Test
    public void testStoreAndLoadWorkflow() throws StoreException {

        ArrayList<Workflow> existingWorkflows = loadExistingWorkflows();

        WorkflowStore workflowStore = storeService.getWorkflowStore();
        String namespace = UUID.randomUUID().toString();
        // test create and load
        ArrayList<Workflow.WorkflowTask> workflowOneTasks = new ArrayList<>();
        workflowOneTasks.add(createWorkflowTask("taskOne", "typeA"));
        workflowOneTasks.add(createWorkflowTask("taskTwo", "typeB"));
        workflowOneTasks.add(createWorkflowTask("taskThree", "typeC"));
        Workflow workflowOne = createWorkflow(namespace, UUID.randomUUID().toString(), workflowOneTasks);

        ArrayList<Workflow.WorkflowTask> workflowTwoTasks = new ArrayList<>();
        workflowTwoTasks.add(createWorkflowTask("taskOne", "typeA"));
        workflowTwoTasks.add(createWorkflowTask("taskTwo", "typeB"));
        workflowTwoTasks.add(createWorkflowTask("taskThree", "typeC"));
        Workflow workflowTwo = createWorkflow(namespace, UUID.randomUUID().toString(), workflowTwoTasks);

        List<Workflow> workflowsByNs = workflowStore.load(namespace);
        assertEquals(workflowsByNs.size(), 2);
        Assert.assertTrue(workflowsByNs.contains(workflowOne));
        Assert.assertTrue(workflowsByNs.contains(workflowTwo));

        assertEquals(workflowOne, workflowStore.load(workflowOne));
        assertEquals(workflowTwo, workflowStore.load(workflowTwo));

        ArrayList<Workflow> workflowsAfterCreate = loadExistingWorkflows();
        workflowsAfterCreate.remove(workflowOne);
        workflowsAfterCreate.remove(workflowTwo);

        Assert.assertTrue("Workflows loaded from store do not match with expected",
                workflowsAfterCreate.size() == existingWorkflows.size() &&
                        workflowsAfterCreate.containsAll(existingWorkflows)
                        && existingWorkflows.containsAll(workflowsAfterCreate));
    }

    @Test
    public void testUpdateAndLoadWorkflow() throws StoreException {
        ArrayList<Workflow> existingWorkflows = loadExistingWorkflows();

        WorkflowStore workflowStore = storeService.getWorkflowStore();
        ArrayList<Workflow.WorkflowTask> workflowTasks = new ArrayList<>();
        workflowTasks.add(createWorkflowTask("taskOne", "typeA"));
        workflowTasks.add(createWorkflowTask("taskTwo", "typeB"));
        workflowTasks.add(createWorkflowTask("taskThree", "typeC"));
        Workflow workflow = createWorkflow(UUID.randomUUID().toString(), UUID.randomUUID().toString(), workflowTasks);
        workflow.getTasks().add(createWorkflowTask("taskFour", "typeD"));
        workflowStore.update(workflow);
        assertEquals(workflow, workflowStore.load(workflow));

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("keyA", "valA");
        properties.put("keyB", "valB");
        properties.put("keyC", "valC");
        workflow.setProperties(properties);
        workflow.getTasks().get(0).setProperties(properties);
        workflowStore.update(workflow);
        assertEquals(workflow, workflowStore.load(workflow));

        workflow.setDescription("new description");
        workflowStore.update(workflow);
        assertEquals(workflow, workflowStore.load(workflow));

        workflow.setEmailOnFailure(Collections.singletonList("oss.failure.new@cognitree.com"));
        workflowStore.update(workflow);
        assertEquals(workflow, workflowStore.load(workflow));

        workflow.setEmailOnSuccess(Collections.singletonList("oss.success.new@cognitree.com"));
        workflowStore.update(workflow);
        assertEquals(workflow, workflowStore.load(workflow));

        ArrayList<Workflow> workflowsAfterCreate = loadExistingWorkflows();
        workflowsAfterCreate.remove(workflow);
        Assert.assertTrue("Workflows loaded from store do not match with expected",
                workflowsAfterCreate.size() == existingWorkflows.size() &&
                        workflowsAfterCreate.containsAll(existingWorkflows)
                        && existingWorkflows.containsAll(workflowsAfterCreate));
    }

    @Test
    public void testDeleteWorkflow() throws StoreException {
        ArrayList<Workflow> existingWorkflows = loadExistingWorkflows();
        WorkflowStore workflowStore = storeService.getWorkflowStore();
        ArrayList<Workflow.WorkflowTask> workflowTasks = new ArrayList<>();
        workflowTasks.add(createWorkflowTask("taskOne", "typeA"));
        workflowTasks.add(createWorkflowTask("taskTwo", "typeB"));
        workflowTasks.add(createWorkflowTask("taskThree", "typeC"));
        Workflow workflow = createWorkflow(UUID.randomUUID().toString(), UUID.randomUUID().toString(), workflowTasks);
        assertEquals(workflow, workflowStore.load(workflow));
        workflowStore.delete(workflow);
        Assert.assertNull(workflowStore.load(workflow));
        ArrayList<Workflow> workflowsAfterCreate = loadExistingWorkflows();
        Assert.assertTrue("Workflows loaded from store do not match with expected",
                workflowsAfterCreate.size() == existingWorkflows.size() &&
                        workflowsAfterCreate.containsAll(existingWorkflows)
                        && existingWorkflows.containsAll(workflowsAfterCreate));
    }

    private ArrayList<Workflow> loadExistingWorkflows() throws StoreException {
        WorkflowStore workflowStore = storeService.getWorkflowStore();
        ArrayList<Workflow> existingWorkflows = new ArrayList<>();
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        List<Namespace> namespaces = namespaceStore.load();
        namespaces.forEach(namespace -> {
            try {
                List<Workflow> workflows = workflowStore.load(namespace.getName());
                if (workflows != null && !workflows.isEmpty()) {
                    existingWorkflows.addAll(workflows);
                }
            } catch (StoreException e) {
                Assert.fail(e.getMessage());
            }
        });
        return existingWorkflows;
    }
}
