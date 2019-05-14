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

import com.cognitree.kronos.model.Policy;
import com.cognitree.kronos.model.RetryPolicy;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.TaskStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.cognitree.kronos.model.Task.Status.CREATED;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SKIPPED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.model.Task.Status.UP_FOR_RETRY;
import static com.cognitree.kronos.model.Task.Status.WAITING;

public class TaskStoreTest extends StoreTest {

    @Before
    public void setup() throws Exception {
        // initialize store with some namespaces
        for (int i = 0; i < 5; i++) {
            createTask(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
    }

    @Test
    public void testStoreAndLoadTasks() throws StoreException {
        ArrayList<Task> existingTasks = loadExistingTasks();
        TaskStore taskStore = storeService.getTaskStore();
        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        Task taskOne = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        Task taskTwo = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        List<Task> tasksByNs = taskStore.load(namespace);
        assertEquals(2, tasksByNs.size());
        Assert.assertTrue(tasksByNs.contains(taskOne));
        Assert.assertTrue(tasksByNs.contains(taskTwo));
        assertEquals(taskOne, taskStore.load(taskOne));
        assertEquals(taskTwo, taskStore.load(taskTwo));

        List<Task> tasksByStatusCreated = taskStore.loadByStatus(namespace, Collections.singletonList(CREATED));
        assertEquals(2, tasksByStatusCreated.size());
        Assert.assertTrue(tasksByStatusCreated.contains(taskOne));
        Assert.assertTrue(tasksByStatusCreated.contains(taskTwo));

        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(RUNNING)));
        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(WAITING)));
        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(SCHEDULED)));
        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(SUBMITTED)));
        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(FAILED)));
        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(SKIPPED)));
        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(UP_FOR_RETRY)));
        assertEquals(Collections.emptyList(), taskStore.loadByStatus(namespace, Collections.singletonList(SUCCESSFUL)));

        List<Task> tasksByWorkflowName = taskStore.loadByJobIdAndWorkflowName(namespace, job, workflow);
        assertEquals(tasksByWorkflowName.size(), 2);
        Assert.assertTrue(tasksByWorkflowName.contains(taskOne));
        Assert.assertTrue(tasksByWorkflowName.contains(taskTwo));

        Map<Task.Status, Integer> countTasksByStatus = taskStore.countByStatus(namespace, 0, System.currentTimeMillis());
        assertEquals(2, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());
        assertEquals(0, countTasksByStatus.get(WAITING) == null ? 0 : countTasksByStatus.get(WAITING).intValue());
        assertEquals(0, countTasksByStatus.get(SCHEDULED) == null ? 0 : countTasksByStatus.get(SCHEDULED).intValue());
        assertEquals(0, countTasksByStatus.get(SUBMITTED) == null ? 0 : countTasksByStatus.get(SUBMITTED).intValue());
        assertEquals(0, countTasksByStatus.get(RUNNING) == null ? 0 : countTasksByStatus.get(RUNNING).intValue());
        assertEquals(0, countTasksByStatus.get(SKIPPED) == null ? 0 : countTasksByStatus.get(SKIPPED).intValue());
        assertEquals(0, countTasksByStatus.get(FAILED) == null ? 0 : countTasksByStatus.get(FAILED).intValue());
        assertEquals(0, countTasksByStatus.get(SUCCESSFUL) == null ? 0 : countTasksByStatus.get(SUCCESSFUL).intValue());
        assertEquals(0, countTasksByStatus.get(UP_FOR_RETRY) == null ? 0 : countTasksByStatus.get(UP_FOR_RETRY).intValue());

        ArrayList<Task> tasksAfterCreate = loadExistingTasks();
        tasksAfterCreate.remove(taskOne);
        tasksAfterCreate.remove(taskTwo);
        Assert.assertTrue("Tasks loaded from store do not match with expected",
                tasksAfterCreate.size() == existingTasks.size() &&
                        tasksAfterCreate.containsAll(existingTasks)
                        && existingTasks.containsAll(tasksAfterCreate));
    }

    @Test
    public void testUpdateTask() throws StoreException {
        ArrayList<Task> existingTasks = loadExistingTasks();
        TaskStore taskStore = storeService.getTaskStore();
        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        Task task = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        List<Task> tasksByNs = taskStore.load(namespace);
        assertEquals(1, tasksByNs.size());
        Assert.assertTrue(tasksByNs.contains(task));
        assertEquals(task, taskStore.load(task));

        task.setStatus(SUCCESSFUL);
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        task.setRetryCount(2);
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("A", "taskA");
        properties.put("B", "taskB");
        task.setProperties(properties);
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        task.setCompletedAt(System.currentTimeMillis());
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        task.setSubmittedAt(System.currentTimeMillis());
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        task.setStatusMessage("status message");
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        ArrayList<Policy> policies = new ArrayList<>();
        RetryPolicy retryPolicy = new RetryPolicy();
        retryPolicy.setMaxRetryCount(9);
        task.setPolicies(policies);
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        HashMap<String, Object> context = new HashMap<>();
        context.put("cA", "taskA");
        context.put("cB", "taskB");
        task.setContext(context);
        taskStore.update(task);
        assertEquals(task, taskStore.load(task));

        ArrayList<Task> tasksAfterCreate = loadExistingTasks();
        tasksAfterCreate.remove(task);
        Assert.assertTrue("Tasks loaded from store do not match with expected",
                tasksAfterCreate.size() == existingTasks.size() &&
                        tasksAfterCreate.containsAll(existingTasks)
                        && existingTasks.containsAll(tasksAfterCreate));
    }

    @Test
    public void testCountTaskByStatus() throws StoreException {
        ArrayList<Task> existingTasks = loadExistingTasks();
        TaskStore taskStore = storeService.getTaskStore();
        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        Task taskOne = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        Task taskTwo = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        Task taskThree = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        Task taskFour = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        Task taskFive = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        Task taskSix = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        List<Task> tasksByNs = taskStore.load(namespace);
        assertEquals(6, tasksByNs.size());

        Map<Task.Status, Integer> countTasksByStatus = taskStore.countByStatus(namespace, 0, System.currentTimeMillis());
        assertEquals(6, countTasksByStatus.get(CREATED).intValue());
        assertEquals(0, countTasksByStatus.get(WAITING) == null ? 0 : countTasksByStatus.get(WAITING).intValue());
        assertEquals(0, countTasksByStatus.get(SCHEDULED) == null ? 0 : countTasksByStatus.get(SCHEDULED).intValue());
        assertEquals(0, countTasksByStatus.get(SUBMITTED) == null ? 0 : countTasksByStatus.get(SUBMITTED).intValue());
        assertEquals(0, countTasksByStatus.get(RUNNING) == null ? 0 : countTasksByStatus.get(RUNNING).intValue());
        assertEquals(0, countTasksByStatus.get(SKIPPED) == null ? 0 : countTasksByStatus.get(SKIPPED).intValue());
        assertEquals(0, countTasksByStatus.get(FAILED) == null ? 0 : countTasksByStatus.get(FAILED).intValue());
        assertEquals(0, countTasksByStatus.get(SUCCESSFUL) == null ? 0 : countTasksByStatus.get(SUCCESSFUL).intValue());
        assertEquals(0, countTasksByStatus.get(UP_FOR_RETRY) == null ? 0 : countTasksByStatus.get(UP_FOR_RETRY).intValue());

        taskOne.setStatus(RUNNING);
        taskStore.update(taskOne);
        taskTwo.setStatus(SUCCESSFUL);
        taskStore.update(taskTwo);
        taskThree.setStatus(FAILED);
        taskStore.update(taskThree);
        taskFour.setStatus(SCHEDULED);
        taskStore.update(taskFour);
        taskFive.setStatus(SUBMITTED);
        taskStore.update(taskFive);
        taskSix.setStatus(UP_FOR_RETRY);
        taskStore.update(taskSix);

        countTasksByStatus = taskStore.countByStatus(namespace, 0, System.currentTimeMillis());
        assertEquals(0, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());
        assertEquals(0, countTasksByStatus.get(WAITING) == null ? 0 : countTasksByStatus.get(WAITING).intValue());
        assertEquals(1, countTasksByStatus.get(SCHEDULED) == null ? 0 : countTasksByStatus.get(SCHEDULED).intValue());
        assertEquals(1, countTasksByStatus.get(SUBMITTED) == null ? 0 : countTasksByStatus.get(SUBMITTED).intValue());
        assertEquals(1, countTasksByStatus.get(RUNNING) == null ? 0 : countTasksByStatus.get(RUNNING).intValue());
        assertEquals(0, countTasksByStatus.get(SKIPPED) == null ? 0 : countTasksByStatus.get(SKIPPED).intValue());
        assertEquals(1, countTasksByStatus.get(FAILED) == null ? 0 : countTasksByStatus.get(FAILED).intValue());
        assertEquals(1, countTasksByStatus.get(SUCCESSFUL) == null ? 0 : countTasksByStatus.get(SUCCESSFUL).intValue());
        assertEquals(1, countTasksByStatus.get(UP_FOR_RETRY) == null ? 0 : countTasksByStatus.get(UP_FOR_RETRY).intValue());

        taskOne.setStatus(WAITING);
        taskStore.update(taskOne);
        taskFour.setStatus(SKIPPED);
        taskStore.update(taskFour);

        countTasksByStatus = taskStore.countByStatus(namespace, 0, System.currentTimeMillis());
        assertEquals(0, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());
        assertEquals(1, countTasksByStatus.get(WAITING) == null ? 0 : countTasksByStatus.get(WAITING).intValue());
        assertEquals(0, countTasksByStatus.get(SCHEDULED) == null ? 0 : countTasksByStatus.get(SCHEDULED).intValue());
        assertEquals(1, countTasksByStatus.get(SUBMITTED) == null ? 0 : countTasksByStatus.get(SUBMITTED).intValue());
        assertEquals(0, countTasksByStatus.get(RUNNING) == null ? 0 : countTasksByStatus.get(RUNNING).intValue());
        assertEquals(1, countTasksByStatus.get(SKIPPED) == null ? 0 : countTasksByStatus.get(SKIPPED).intValue());
        assertEquals(1, countTasksByStatus.get(FAILED) == null ? 0 : countTasksByStatus.get(FAILED).intValue());
        assertEquals(1, countTasksByStatus.get(SUCCESSFUL) == null ? 0 : countTasksByStatus.get(SUCCESSFUL).intValue());
        assertEquals(1, countTasksByStatus.get(UP_FOR_RETRY) == null ? 0 : countTasksByStatus.get(UP_FOR_RETRY).intValue());

        ArrayList<Task> tasksAfterCreate = loadExistingTasks();
        tasksAfterCreate.remove(taskOne);
        tasksAfterCreate.remove(taskTwo);
        tasksAfterCreate.remove(taskThree);
        tasksAfterCreate.remove(taskFour);
        tasksAfterCreate.remove(taskFive);
        tasksAfterCreate.remove(taskSix);
        Assert.assertTrue("Tasks loaded from store do not match with expected",
                tasksAfterCreate.size() == existingTasks.size() &&
                        tasksAfterCreate.containsAll(existingTasks)
                        && existingTasks.containsAll(tasksAfterCreate));
    }

    @Test
    public void testCountTaskByStatusCreatedAfterAndBefore() throws StoreException {
        ArrayList<Task> existingTasks = loadExistingTasks();
        TaskStore taskStore = storeService.getTaskStore();
        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Task taskOne = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);
        Task taskTwo = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt + 100);
        Task taskThree = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);
        Task taskFour = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt - 100);
        Task taskFive = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);
        Task taskSix = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);
        List<Task> tasksByNs = taskStore.load(namespace);
        assertEquals(6, tasksByNs.size());

        Map<Task.Status, Integer> countTasksByStatus = taskStore.countByStatus(namespace, createdAt, createdAt + 100);
        assertEquals(0, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        countTasksByStatus = taskStore.countByStatus(namespace, createdAt - 100, createdAt);
        assertEquals(0, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        countTasksByStatus = taskStore.countByStatus(namespace, createdAt - 1, createdAt + 1);
        assertEquals(4, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        Task taskSeven = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);

        countTasksByStatus = taskStore.countByStatus(namespace, createdAt - 1, createdAt + 1);
        assertEquals(5, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        ArrayList<Task> tasksAfterCreate = loadExistingTasks();
        tasksAfterCreate.remove(taskOne);
        tasksAfterCreate.remove(taskTwo);
        tasksAfterCreate.remove(taskThree);
        tasksAfterCreate.remove(taskFour);
        tasksAfterCreate.remove(taskFive);
        tasksAfterCreate.remove(taskSix);
        tasksAfterCreate.remove(taskSeven);
        Assert.assertTrue("Tasks loaded from store do not match with expected",
                tasksAfterCreate.size() == existingTasks.size() &&
                        tasksAfterCreate.containsAll(existingTasks)
                        && existingTasks.containsAll(tasksAfterCreate));
    }

    @Test
    public void testCountTaskByStatusCreatedAfterAndBeforeAndWorkflowName() throws StoreException {
        ArrayList<Task> existingTasks = loadExistingTasks();
        TaskStore taskStore = storeService.getTaskStore();
        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Task taskOne = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);
        Task taskTwo = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt + 100);
        Task taskThree = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);
        Task taskFour = createTask(namespace, UUID.randomUUID().toString(), trigger, job, UUID.randomUUID().toString(), createdAt);
        Task taskFive = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt - 100);
        Task taskSix = createTask(namespace, UUID.randomUUID().toString(), trigger, job, UUID.randomUUID().toString(), createdAt);
        List<Task> tasksByNs = taskStore.load(namespace);
        assertEquals(6, tasksByNs.size());

        Map<Task.Status, Integer> countTasksByStatus = taskStore.countByStatusForWorkflowName(namespace, workflow,
                createdAt - 100, createdAt);
        assertEquals(0, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        countTasksByStatus = taskStore.countByStatusForWorkflowName(namespace, workflow, createdAt, createdAt + 100);
        assertEquals(0, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        countTasksByStatus = taskStore.countByStatusForWorkflowName(namespace, workflow, createdAt - 1, createdAt + 1);
        assertEquals(2, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        Task taskSeven = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString(), createdAt);

        countTasksByStatus = taskStore.countByStatusForWorkflowName(namespace, workflow, createdAt - 1, createdAt + 1);
        assertEquals(3, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        countTasksByStatus = taskStore.countByStatusForWorkflowName(namespace, taskFour.getWorkflow(), createdAt - 1,
                createdAt + 1);
        assertEquals(1, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        countTasksByStatus = taskStore.countByStatusForWorkflowName(namespace, taskSix.getWorkflow(), createdAt - 1,
                createdAt + 1);
        assertEquals(1, countTasksByStatus.get(CREATED) == null ? 0 : countTasksByStatus.get(CREATED).intValue());

        ArrayList<Task> tasksAfterCreate = loadExistingTasks();
        tasksAfterCreate.remove(taskOne);
        tasksAfterCreate.remove(taskTwo);
        tasksAfterCreate.remove(taskThree);
        tasksAfterCreate.remove(taskFour);
        tasksAfterCreate.remove(taskFive);
        tasksAfterCreate.remove(taskSix);
        tasksAfterCreate.remove(taskSeven);
        Assert.assertTrue("Tasks loaded from store do not match with expected",
                tasksAfterCreate.size() == existingTasks.size() &&
                        tasksAfterCreate.containsAll(existingTasks)
                        && existingTasks.containsAll(tasksAfterCreate));
    }

    @Test
    public void testDeleteTask() throws StoreException {
        ArrayList<Task> existingTasks = loadExistingTasks();
        TaskStore taskStore = storeService.getTaskStore();
        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        String job = UUID.randomUUID().toString();
        Task task = createTask(namespace, workflow, trigger, job, UUID.randomUUID().toString());
        List<Task> tasksByNs = taskStore.load(namespace);
        assertEquals(1, tasksByNs.size());
        Assert.assertTrue(tasksByNs.contains(task));

        taskStore.delete(task);
        ArrayList<Task> tasksAfterCreate = loadExistingTasks();
        Assert.assertTrue("Tasks loaded from store do not match with expected",
                tasksAfterCreate.size() == existingTasks.size() &&
                        tasksAfterCreate.containsAll(existingTasks)
                        && existingTasks.containsAll(tasksAfterCreate));
    }

    private ArrayList<Task> loadExistingTasks() throws StoreException {
        TaskStore taskStore = storeService.getTaskStore();
        ArrayList<Task> existingTasks = new ArrayList<>();
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        List<Namespace> namespaces = namespaceStore.load();
        namespaces.forEach(namespace -> {
            try {
                List<Task> tasks = taskStore.load(namespace.getName());
                if (tasks != null && !tasks.isEmpty()) {
                    existingTasks.addAll(tasks);
                }
            } catch (StoreException e) {
                Assert.fail(e.getMessage());
            }
        });
        return existingTasks;
    }
}
