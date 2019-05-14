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

import com.cognitree.kronos.scheduler.model.CronSchedule;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.SimpleSchedule;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class WorkflowTriggerTest extends StoreTest {

    @Before
    public void setup() throws Exception {
        for (int i = 0; i < 5; i++) {
            createWorkflowTrigger(UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(), new SimpleSchedule());
        }
    }

    @Test
    public void testCreateAndLoadWorkflowTrigger() throws StoreException {
        ArrayList<WorkflowTrigger> existingWorkflowTriggers = loadExistingWorkflowTriggers();

        WorkflowTriggerStore workflowTriggerStore = storeService.getWorkflowTriggerStore();

        SimpleSchedule simpleSchedule = new SimpleSchedule();
        simpleSchedule.setRepeatIntervalInMs(1000);
        simpleSchedule.setRepeatForever(false);
        simpleSchedule.setRepeatCount(100);
        String namespace = UUID.randomUUID().toString();
        String workflowName = UUID.randomUUID().toString();
        WorkflowTrigger workflowTriggerOne = createWorkflowTrigger(namespace, workflowName,
                UUID.randomUUID().toString(), simpleSchedule);

        CronSchedule cronSchedule = new CronSchedule();
        cronSchedule.setCronExpression("0/2 * * * * ?");
        WorkflowTrigger workflowTriggerTwo = createWorkflowTrigger(namespace, workflowName,
                UUID.randomUUID().toString(), cronSchedule);

        assertEquals(workflowTriggerOne, workflowTriggerStore.load(workflowTriggerOne));
        assertEquals(workflowTriggerTwo, workflowTriggerStore.load(workflowTriggerTwo));

        List<WorkflowTrigger> workflowTriggersByNs = workflowTriggerStore.load(workflowTriggerOne.getNamespace());
        assertEquals(2, workflowTriggersByNs.size());
        Assert.assertTrue(workflowTriggersByNs.contains(workflowTriggerOne));
        Assert.assertTrue(workflowTriggersByNs.contains(workflowTriggerTwo));
        List<WorkflowTrigger> workflowTriggersByWfName = workflowTriggerStore.loadByWorkflowName(namespace, workflowName);
        assertEquals(2, workflowTriggersByWfName.size());
        Assert.assertTrue(workflowTriggersByWfName.contains(workflowTriggerOne));
        Assert.assertTrue(workflowTriggersByWfName.contains(workflowTriggerTwo));

        List<WorkflowTrigger> workflowTriggersByWfNameAndEnabled =
                workflowTriggerStore.loadByWorkflowNameAndEnabled(namespace, workflowName, true);
        assertEquals(2, workflowTriggersByWfNameAndEnabled.size());
        Assert.assertTrue(workflowTriggersByWfNameAndEnabled.contains(workflowTriggerOne));
        Assert.assertTrue(workflowTriggersByWfNameAndEnabled.contains(workflowTriggerTwo));

        List<WorkflowTrigger> workflowTriggersByWfNameAndDisabled =
                workflowTriggerStore.loadByWorkflowNameAndEnabled(namespace, workflowName, false);
        assertEquals(0, workflowTriggersByWfNameAndDisabled.size());

        ArrayList<WorkflowTrigger> workflowTriggersAfterCreate = loadExistingWorkflowTriggers();
        workflowTriggersAfterCreate.remove(workflowTriggerOne);
        workflowTriggersAfterCreate.remove(workflowTriggerTwo);

        Assert.assertTrue("Workflow Triggers loaded from store do not match with expected",
                workflowTriggersAfterCreate.size() == existingWorkflowTriggers.size() &&
                        workflowTriggersAfterCreate.containsAll(existingWorkflowTriggers)
                        && existingWorkflowTriggers.containsAll(workflowTriggersAfterCreate));
    }

    @Test
    public void testUpdateWorkflowTrigger() throws StoreException {
        ArrayList<WorkflowTrigger> existingWorkflowTriggers = loadExistingWorkflowTriggers();

        WorkflowTriggerStore workflowTriggerStore = storeService.getWorkflowTriggerStore();
        CronSchedule cronSchedule = new CronSchedule();
        cronSchedule.setCronExpression("0/2 * * * * ?");
        WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), cronSchedule);

        // test update and load
        workflowTrigger.setEnabled(false);
        workflowTriggerStore.update(workflowTrigger);
        assertEquals(workflowTrigger, workflowTriggerStore.load(workflowTrigger));

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("keyA", "valA");
        properties.put("keyB", "valB");
        workflowTrigger.setProperties(properties);
        workflowTriggerStore.update(workflowTrigger);
        assertEquals(workflowTrigger, workflowTriggerStore.load(workflowTrigger));

        workflowTrigger.setStartAt(1000L);
        workflowTrigger.setEndAt(5000L);
        workflowTriggerStore.update(workflowTrigger);
        assertEquals(workflowTrigger, workflowTriggerStore.load(workflowTrigger));

        ArrayList<WorkflowTrigger> workflowTriggersAfterCreate = loadExistingWorkflowTriggers();
        workflowTriggersAfterCreate.remove(workflowTrigger);
        Assert.assertTrue("Workflow Triggers loaded from store do not match with expected",
                workflowTriggersAfterCreate.size() == existingWorkflowTriggers.size() &&
                        workflowTriggersAfterCreate.containsAll(existingWorkflowTriggers)
                        && existingWorkflowTriggers.containsAll(workflowTriggersAfterCreate));
    }

    @Test
    public void testDeleteWorkflowTrigger() throws StoreException {
        ArrayList<WorkflowTrigger> existingWorkflowTriggers = loadExistingWorkflowTriggers();

        WorkflowTriggerStore workflowTriggerStore = storeService.getWorkflowTriggerStore();
        CronSchedule cronSchedule = new CronSchedule();
        cronSchedule.setCronExpression("0/2 * * * * ?");
        WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), cronSchedule);
        assertEquals(workflowTrigger, workflowTriggerStore.load(workflowTrigger));

        workflowTriggerStore.delete(workflowTrigger);
        Assert.assertNull(workflowTriggerStore.load(workflowTrigger));

        ArrayList<WorkflowTrigger> workflowTriggersAfterCreate = loadExistingWorkflowTriggers();
        Assert.assertTrue("Workflow Triggers loaded from store do not match with expected",
                workflowTriggersAfterCreate.size() == existingWorkflowTriggers.size() &&
                        workflowTriggersAfterCreate.containsAll(existingWorkflowTriggers)
                        && existingWorkflowTriggers.containsAll(workflowTriggersAfterCreate));
    }

    private ArrayList<WorkflowTrigger> loadExistingWorkflowTriggers() throws StoreException {
        WorkflowTriggerStore workflowTriggerStore = storeService.getWorkflowTriggerStore();
        ArrayList<WorkflowTrigger> workflowTriggers = new ArrayList<>();
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        List<Namespace> namespaces = namespaceStore.load();
        namespaces.forEach(namespace -> {
            try {
                List<WorkflowTrigger> triggers = workflowTriggerStore.load(namespace.getName());
                if (triggers != null && !triggers.isEmpty()) {
                    workflowTriggers.addAll(triggers);
                }
            } catch (StoreException e) {
                Assert.fail(e.getMessage());
            }
        });
        return workflowTriggers;
    }
}
