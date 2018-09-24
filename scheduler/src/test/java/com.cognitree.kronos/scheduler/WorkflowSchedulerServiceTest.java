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

import com.cognitree.kronos.scheduler.model.CronSchedule;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.CronExpression;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class WorkflowSchedulerServiceTest {

    private static final CronSchedule schedule = new CronSchedule();
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();

    static {
        schedule.setCronExpression("0/2 * * * * ?");
    }

    @BeforeClass
    public static void start() throws Exception {
        SCHEDULER_APP.start();
    }

    @AfterClass
    public static void stop() {
        SCHEDULER_APP.stop();
    }

    @Test
    public void testScheduleWorkflow() throws SchedulerException, IOException, ParseException {
        final WorkflowSchedulerService workflowSchedulerService = WorkflowSchedulerService.getService();
        final Workflow workflow = createWorkflow(UUID.randomUUID().toString(),
                UUID.randomUUID().toString());
        workflowSchedulerService.add(workflow);
        final WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflow.getName(), workflow.getNamespace());
        workflowSchedulerService.add(workflowTrigger);
        final Scheduler scheduler = workflowSchedulerService.getScheduler();
        Assert.assertTrue(scheduler.checkExists(workflowSchedulerService.getJobKey(workflow)));
    }

    @Test
    public void testDeleteWorkflow() throws SchedulerException, IOException {
        final WorkflowSchedulerService workflowSchedulerService = WorkflowSchedulerService.getService();
        final Workflow workflow = createWorkflow(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        workflowSchedulerService.add(workflow);
        final Scheduler scheduler = workflowSchedulerService.getScheduler();
        Assert.assertTrue(scheduler.checkExists(workflowSchedulerService.getJobKey(workflow)));
        workflowSchedulerService.delete(workflow);
        Assert.assertFalse(scheduler.checkExists(workflowSchedulerService.getJobKey(workflow)));
    }

    @Test(expected = SchedulerException.class)
    public void testAddWorkflowTriggerWithoutWorkflow() throws SchedulerException, ParseException {
        final WorkflowSchedulerService workflowSchedulerService = WorkflowSchedulerService.getService();
        final WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        workflowSchedulerService.add(workflowTrigger);
        Assert.fail();
    }

    @Test
    public void testDeleteWorkflowTrigger() throws SchedulerException, IOException, ParseException {
        final WorkflowSchedulerService workflowSchedulerService = WorkflowSchedulerService.getService();
        final Workflow workflow = createWorkflow(UUID.randomUUID().toString(),
                UUID.randomUUID().toString());
        workflowSchedulerService.add(workflow);
        final WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflow.getName(), workflow.getNamespace());
        workflowSchedulerService.add(workflowTrigger);
        final Scheduler scheduler = workflowSchedulerService.getScheduler();
        Assert.assertTrue(scheduler.checkExists(workflowSchedulerService.getTriggerKey(workflowTrigger)));
        workflowSchedulerService.delete(workflowTrigger);
        Assert.assertTrue(scheduler.checkExists(workflowSchedulerService.getJobKey(workflow)));
        Assert.assertFalse(scheduler.checkExists(workflowSchedulerService.getTriggerKey(workflowTrigger)));
    }

    @Test
    public void testResolveWorkflowTasks() throws IOException {
        final Workflow workflow = createWorkflow(UUID.randomUUID().toString(),
                UUID.randomUUID().toString());
        final List<Workflow.WorkflowTask> workflowTasks =
                WorkflowSchedulerService.getService().orderWorkflowTasks(workflow.getTasks());
        Assert.assertEquals("taskOne", workflowTasks.get(0).getName());
        Assert.assertEquals("taskTwo", workflowTasks.get(1).getName());
        Assert.assertEquals("taskThree", workflowTasks.get(2).getName());
    }

    private Workflow createWorkflow(String workflowName, String namespace) throws IOException {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("workflow.yaml");
        final Workflow workflow = YAML_MAPPER.readValue(resourceAsStream, Workflow.class);
        workflow.setName(workflowName);
        workflow.setNamespace(namespace);
        return workflow;
    }

    private WorkflowTrigger createWorkflowTrigger(String triggerName, String workflow, String namespace) throws ParseException {
        final long nextFireTime = new CronExpression(schedule.getCronExpression())
                .getNextValidTimeAfter(new Date(System.currentTimeMillis() + 100)).getTime();
        final WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName(triggerName);
        workflowTrigger.setWorkflow(workflow);
        workflowTrigger.setNamespace(namespace);
        workflowTrigger.setSchedule(schedule);
        workflowTrigger.setStartAt(nextFireTime - 100);
        workflowTrigger.setEndAt(nextFireTime + 100);
        return workflowTrigger;
    }

}
