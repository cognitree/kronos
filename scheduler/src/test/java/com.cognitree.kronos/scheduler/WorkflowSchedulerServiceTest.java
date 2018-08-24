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

import com.cognitree.kronos.model.definitions.WorkflowDefinition;
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
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowDefinitionService.getService().validate(workflowDefinition);
    }

    @Test(expected = ValidationException.class)
    public void testInValidWorkflowMissingTasks() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("invalid-workflow-missing-tasks.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowDefinitionService.getService().validate(workflowDefinition);
    }

    @Test(expected = ValidationException.class)
    public void testInValidWorkflowDisabledTaskDependency() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("invalid-workflow-disabled-tasks-dependency.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowDefinitionService.getService().validate(workflowDefinition);
    }

    @Test
    public void testResolveWorkflowTasks() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("valid-workflow.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowDefinitionService.getService().validate(workflowDefinition);
        final List<WorkflowDefinition.WorkflowTask> workflowTasks =
                WorkflowSchedulerService.getService().orderWorkflowTasks(workflowDefinition.getTasks());
        Assert.assertEquals("task one", workflowTasks.get(0).getName());
        Assert.assertEquals("task two", workflowTasks.get(1).getName());
        Assert.assertEquals("task three", workflowTasks.get(2).getName());
    }

    @Test
    public void testScheduleWorkflow() throws Exception {
        TaskSchedulerService.getService().getTaskProvider().reinit();
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("test-workflow.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowDefinitionService.getService().add(workflowDefinition);
        sleep(System.currentTimeMillis() % 2000);
        final JobKey jobKey = new JobKey(workflowDefinition.getName(), workflowDefinition.getNamespace());
        Assert.assertTrue(WorkflowSchedulerService.getService().getScheduler().checkExists(jobKey));
        WorkflowDefinitionService.getService().delete(workflowDefinition);
        Assert.assertFalse(WorkflowSchedulerService.getService().getScheduler().checkExists(jobKey));
        sleep(System.currentTimeMillis() % 2000);
        Assert.assertEquals(3, TaskSchedulerService.getService().getTaskProvider().size());
    }

    @Test
    public void testExecuteWorkflow() throws Exception {
        TaskSchedulerService.getService().getTaskProvider().reinit();
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("test-workflow.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowDefinitionService.getService().validate(workflowDefinition);
        Assert.assertEquals(0, TaskSchedulerService.getService().getTaskProvider().size());
        WorkflowDefinitionService.getService().execute(workflowDefinition);
        sleep(100);
        Assert.assertEquals(3, TaskSchedulerService.getService().getTaskProvider().size());
    }
}
