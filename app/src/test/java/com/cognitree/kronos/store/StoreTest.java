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
import com.cognitree.kronos.scheduler.SchedulerConfig;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Schedule;
import com.cognitree.kronos.scheduler.model.SimpleSchedule;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.StoreServiceConfig;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StoreTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    static StoreService storeService;

    private static List<Namespace> existingNamespaces;

    @BeforeClass
    public static void init() throws Exception {
        final InputStream schedulerConfigAsStream =
                StoreTest.class.getClassLoader().getResourceAsStream("scheduler.yaml");
        SchedulerConfig schedulerConfig = MAPPER.readValue(schedulerConfigAsStream, SchedulerConfig.class);
        StoreServiceConfig storeServiceConfig = schedulerConfig.getStoreServiceConfig();
        storeService = (StoreService) Class.forName(storeServiceConfig.getStoreServiceClass())
                .getConstructor(ObjectNode.class).newInstance(storeServiceConfig.getConfig());
        storeService.init();
        storeService.start();
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        existingNamespaces = namespaceStore.load();
    }

    @AfterClass
    public static void destroy() throws StoreException {
        List<Namespace> namespaces = storeService.getNamespaceStore().load();
        namespaces.removeAll(existingNamespaces);
        cleanupStore(namespaces);
        storeService.stop();
    }

    private static void cleanupStore(List<Namespace> namespaces) throws StoreException {
        for (Namespace namespace : namespaces) {
            TaskStore taskStore = storeService.getTaskStore();
            List<Task> tasks = taskStore.load(namespace.getName());
            for (Task task : tasks) {
                taskStore.delete(task);
            }

            JobStore jobStore = storeService.getJobStore();
            List<Job> jobs = jobStore.load(namespace.getName());
            for (Job job : jobs) {
                jobStore.delete(job);
            }

            WorkflowTriggerStore workflowTriggerStore = storeService.getWorkflowTriggerStore();
            List<WorkflowTrigger> workflowTriggers = workflowTriggerStore.load(namespace.getName());
            for (WorkflowTrigger workflowTrigger : workflowTriggers) {
                workflowTriggerStore.delete(workflowTrigger);
            }

            WorkflowStore workflowStore = storeService.getWorkflowStore();
            List<Workflow> workflows = workflowStore.load(namespace.getName());
            for (Workflow workflow : workflows) {
                workflowStore.delete(workflow);
            }

            NamespaceStore namespaceStore = storeService.getNamespaceStore();
            namespaceStore.delete(Namespace.build(namespace.getName()));
        }
    }

    static Namespace createNamespace(String name) throws StoreException {
        Namespace namespace = new Namespace();
        namespace.setName(name);
        if (storeService.getNamespaceStore().load(namespace) == null) {
            storeService.getNamespaceStore().store(namespace);
        }
        return namespace;
    }

    static Workflow createWorkflow(String namespace, String name, List<Workflow.WorkflowTask> tasks)
            throws StoreException {
        createNamespace(namespace);
        Workflow workflow = new Workflow();
        workflow.setNamespace(namespace);
        workflow.setName(name);
        workflow.setDescription("test");
        workflow.setTasks(tasks);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("A", "A");
        properties.put("B", "B");
        workflow.setProperties(properties);
        ArrayList<String> emailOnSuccess = new ArrayList<>();
        emailOnSuccess.add("oss@cognitree.com");
        workflow.setEmailOnSuccess(emailOnSuccess);
        ArrayList<String> emailOnFailures = new ArrayList<>();
        emailOnFailures.add("oss@cognitree.com");
        workflow.setEmailOnFailure(emailOnFailures);
        if (storeService.getWorkflowStore().load(workflow) == null) {
            storeService.getWorkflowStore().store(workflow);
        }
        return workflow;
    }

    static Workflow.WorkflowTask createWorkflowTask(String name, String taskType) {
        Workflow.WorkflowTask workflowTask = new Workflow.WorkflowTask();
        workflowTask.setName(name);
        workflowTask.setType(taskType);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("A", "A");
        properties.put("B", "B");
        workflowTask.setProperties(properties);
        ArrayList<String> dependsOn = new ArrayList<>();
        dependsOn.add("taskOne");
        dependsOn.add("taskTwo");
        workflowTask.setDependsOn(dependsOn);
        ArrayList<Policy> policies = new ArrayList<>();
        RetryPolicy retryPolicy = new RetryPolicy();
        retryPolicy.setMaxRetryCount(4);
        policies.add(retryPolicy);
        workflowTask.setPolicies(policies);
        workflowTask.setMaxExecutionTimeInMs(1000000);
        workflowTask.setEnabled(true);
        return workflowTask;
    }


    static WorkflowTrigger createWorkflowTrigger(String namespace, String workflowName, String name, Schedule schedule) throws StoreException {
        ArrayList<Workflow.WorkflowTask> workflowTasks = new ArrayList<>();
        workflowTasks.add(createWorkflowTask("taskOne", "typeA"));
        workflowTasks.add(createWorkflowTask("taskTwo", "typeB"));
        workflowTasks.add(createWorkflowTask("taskThree", "typeC"));
        createWorkflow(namespace, workflowName, workflowTasks);
        WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName(name);
        workflowTrigger.setNamespace(namespace);
        workflowTrigger.setWorkflow(workflowName);
        workflowTrigger.setSchedule(schedule);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("A", "A");
        properties.put("B", "B");
        workflowTrigger.setProperties(properties);
        workflowTrigger.setStartAt(0L);
        workflowTrigger.setEndAt(System.currentTimeMillis());
        if (storeService.getWorkflowTriggerStore().load(workflowTrigger) == null) {
            storeService.getWorkflowTriggerStore().store(workflowTrigger);
        }
        return workflowTrigger;
    }

    static Job createJob(String namespace, String workflow, String trigger, String id) throws StoreException {
        return createJob(namespace, workflow, trigger, id, System.currentTimeMillis() - 100);
    }

    static Job createJob(String namespace, String workflow, String trigger, String id,
                         long createdAt) throws StoreException {
        createWorkflowTrigger(namespace, workflow, trigger, new SimpleSchedule());
        Job job = new Job();
        job.setId(id);
        job.setNamespace(namespace);
        job.setWorkflow(workflow);
        job.setTrigger(trigger);
        job.setCreatedAt(createdAt);
        if (storeService.getJobStore().load(job) == null) {
            storeService.getJobStore().store(job);
        }
        return job;
    }

    static Task createTask(String namespace, String workflow, String trigger, String job, String name) throws StoreException {
        return createTask(namespace, workflow, trigger, job, name, System.currentTimeMillis() - 100);
    }

    static Task createTask(String namespace, String workflow, String trigger, String job, String name,
                           long createdAt) throws StoreException {
        createJob(namespace, workflow, trigger, job);
        Task task = new Task();
        task.setType("test");
        task.setNamespace(namespace);
        task.setWorkflow(workflow);
        task.setJob(job);
        task.setName(name);
        task.setCreatedAt(createdAt);
        if (storeService.getTaskStore().load(task) == null) {
            storeService.getTaskStore().store(task);
        }
        return task;
    }

    static void assertEquals(Object expected, Object actual) {
        try {
            Assert.assertEquals(MAPPER.writeValueAsString(expected), MAPPER.writeValueAsString(actual));
        } catch (JsonProcessingException e) {
            Assert.fail();
        }
    }
}
