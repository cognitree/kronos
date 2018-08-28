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

import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowTrigger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.JobKey;

import java.io.InputStream;
import java.util.List;

import static java.lang.Thread.sleep;

public class WorkflowSchedulerServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();

    @BeforeClass
    public static void init() throws Exception {
        SCHEDULER_APP.start();
    }

    @AfterClass
    public static void stop() {
        SCHEDULER_APP.stop();
    }

    @Test
    public void testValidWorkflow() throws Exception {
        final InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("valid-workflow.yaml");
        final Workflow workflow = MAPPER.readValue(resourceAsStream, Workflow.class);
        WorkflowService.getService().validate(workflow);
    }

    @Test(expected = ValidationException.class)
    public void testInValidWorkflowMissingTasks() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("invalid-workflow-missing-tasks.yaml");
        final Workflow workflow = MAPPER.readValue(resourceAsStream, Workflow.class);
        WorkflowService.getService().validate(workflow);
    }

    @Test(expected = ValidationException.class)
    public void testInValidWorkflowDisabledTaskDependency() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("invalid-workflow-disabled-tasks-dependency.yaml");
        final Workflow workflow = MAPPER.readValue(resourceAsStream, Workflow.class);
        WorkflowService.getService().validate(workflow);
    }

    @Test
    public void testResolveWorkflowTasks() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("valid-workflow.yaml");
        final Workflow workflow = MAPPER.readValue(resourceAsStream, Workflow.class);
        WorkflowService.getService().validate(workflow);
        final List<Workflow.WorkflowTask> workflowTasks =
                WorkflowSchedulerService.getService().orderWorkflowTasks(workflow.getTasks());
        Assert.assertEquals("task one", workflowTasks.get(0).getName());
        Assert.assertEquals("task two", workflowTasks.get(1).getName());
        Assert.assertEquals("task three", workflowTasks.get(2).getName());
    }

    @Test
    public void testScheduleWorkflow() throws Exception {
        TaskSchedulerService.getService().getTaskProvider().reinit();
        final InputStream workflowDefInputStream =
                getClass().getClassLoader().getResourceAsStream("test-workflow.yaml");
        final Workflow workflow = MAPPER.readValue(workflowDefInputStream, Workflow.class);
        WorkflowService.getService().add(workflow);
        final WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName("test-workflow-trigger");
        workflowTrigger.setWorkflowName(workflow.getName());
        workflowTrigger.setNamespace(workflow.getNamespace());
        workflowTrigger.setSchedule("0/2 * * * * ?");
        final long currentTimeMillis = System.currentTimeMillis();
        workflowTrigger.setStartAt(currentTimeMillis);
        workflowTrigger.setEndAt(currentTimeMillis + 2000 - (currentTimeMillis % 2000));
        WorkflowTriggerService.getService().add(workflowTrigger);
        final JobKey jobKey = new JobKey(workflowTrigger.getName(),
                workflowTrigger.getWorkflowName() + ":" + workflowTrigger.getNamespace());
        Assert.assertTrue(WorkflowSchedulerService.getService().getScheduler().checkExists(jobKey));
        sleep(2000);
        Assert.assertFalse(WorkflowSchedulerService.getService().getScheduler().checkExists(jobKey));
    }

    @Test
    public void testExecuteWorkflow() throws Exception {
        TaskSchedulerService.getService().getTaskProvider().reinit();
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("test-workflow.yaml");
        final Workflow workflow = MAPPER.readValue(resourceAsStream, Workflow.class);
        WorkflowService.getService().validate(workflow);
        Assert.assertEquals(0, TaskSchedulerService.getService().getTaskProvider().size());
        final WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName("test-workflow-trigger");
        workflowTrigger.setWorkflowName(workflow.getName());
        workflowTrigger.setNamespace(workflow.getNamespace());
        workflowTrigger.setSchedule("0/2 * * * * ?");
        WorkflowSchedulerService.getService().execute(workflow, workflowTrigger);
        sleep(100);
        Assert.assertEquals(3, TaskSchedulerService.getService().getTaskProvider().size());
    }
}
