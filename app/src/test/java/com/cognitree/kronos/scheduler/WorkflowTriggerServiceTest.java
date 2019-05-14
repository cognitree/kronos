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
import com.cognitree.kronos.scheduler.model.CalendarIntervalSchedule;
import com.cognitree.kronos.scheduler.model.DailyTimeIntervalSchedule;
import com.cognitree.kronos.scheduler.model.FixedDelaySchedule;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.SimpleSchedule;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.DateBuilder;
import org.quartz.Scheduler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.TestUtil.createNamespace;
import static com.cognitree.kronos.TestUtil.createWorkflow;
import static com.cognitree.kronos.TestUtil.createWorkflowTrigger;
import static com.cognitree.kronos.TestUtil.scheduleWorkflow;
import static com.cognitree.kronos.TestUtil.waitForTriggerToComplete;

public class WorkflowTriggerServiceTest extends ServiceTest {

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
    public void testAddWorkflowTriggerWithSimpleSchedule() throws Exception {
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespace);

        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespace.getName());
        WorkflowService.getService().add(workflow);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger simpleWorkflowTrigger = new WorkflowTrigger();
        simpleWorkflowTrigger.setStartAt(System.currentTimeMillis());
        simpleWorkflowTrigger.setEndAt(System.currentTimeMillis() + 100);
        simpleWorkflowTrigger.setEnabled(true);
        simpleWorkflowTrigger.setWorkflow(workflow.getName());
        simpleWorkflowTrigger.setNamespace(workflow.getNamespace());
        simpleWorkflowTrigger.setName(UUID.randomUUID().toString());
        final SimpleSchedule simpleSchedule = new SimpleSchedule();
        simpleSchedule.setRepeatCount(4);
        simpleSchedule.setRepeatForever(false);
        simpleSchedule.setRepeatIntervalInMs(20000);
        simpleWorkflowTrigger.setSchedule(simpleSchedule);
        workflowTriggerService.add(simpleWorkflowTrigger);
        final WorkflowTrigger workflowTrigger = workflowTriggerService.get(simpleWorkflowTrigger);
        Assert.assertNotNull(workflowTrigger);
        Assert.assertEquals(simpleWorkflowTrigger, workflowTrigger);
    }

    @Test
    public void testAddWorkflowTriggerWithFixedSchedule() throws Exception {
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespace);

        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespace.getName());
        WorkflowService.getService().add(workflow);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger fixedWorkflowTrigger = new WorkflowTrigger();
        fixedWorkflowTrigger.setStartAt(System.currentTimeMillis());
        fixedWorkflowTrigger.setEndAt(System.currentTimeMillis() + 5000);
        fixedWorkflowTrigger.setEnabled(true);
        fixedWorkflowTrigger.setWorkflow(workflow.getName());
        fixedWorkflowTrigger.setNamespace(workflow.getNamespace());
        fixedWorkflowTrigger.setName(UUID.randomUUID().toString());
        final FixedDelaySchedule fixedDelaySchedule = new FixedDelaySchedule();
        fixedDelaySchedule.setIntervalInMs(4);
        fixedDelaySchedule.setIntervalInMs(1000);
        fixedWorkflowTrigger.setSchedule(fixedDelaySchedule);
        workflowTriggerService.add(fixedWorkflowTrigger);
        final WorkflowTrigger workflowTrigger = workflowTriggerService.get(fixedWorkflowTrigger);
        Assert.assertNotNull(workflowTrigger);
        Assert.assertEquals(fixedWorkflowTrigger, workflowTrigger);
    }

    @Test
    public void testAddWorkflowTriggerWithCalendarSchedule() throws Exception {
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespace);

        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespace.getName());
        WorkflowService.getService().add(workflow);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger calWorkflowTrigger = new WorkflowTrigger();
        calWorkflowTrigger.setStartAt(System.currentTimeMillis());
        calWorkflowTrigger.setEndAt(System.currentTimeMillis() + 100);
        calWorkflowTrigger.setEnabled(true);
        calWorkflowTrigger.setWorkflow(workflow.getName());
        calWorkflowTrigger.setNamespace(workflow.getNamespace());
        calWorkflowTrigger.setName(UUID.randomUUID().toString());
        final CalendarIntervalSchedule calendarIntervalSchedule = new CalendarIntervalSchedule();
        calendarIntervalSchedule.setRepeatInterval(3);
        calendarIntervalSchedule.setRepeatIntervalUnit(DateBuilder.IntervalUnit.MINUTE);
        calendarIntervalSchedule.setTimezone("UTC");
        calWorkflowTrigger.setSchedule(calendarIntervalSchedule);
        workflowTriggerService.add(calWorkflowTrigger);
        final WorkflowTrigger workflowTrigger = workflowTriggerService.get(calWorkflowTrigger);
        Assert.assertNotNull(workflowTrigger);
        Assert.assertEquals(calWorkflowTrigger, workflowTrigger);
    }

    @Test
    public void testAddWorkflowTriggerWithDailyTimeSchedule() throws Exception {
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespace);

        final Workflow workflow = createWorkflow("workflows/workflow-template.yaml",
                UUID.randomUUID().toString(), namespace.getName());
        WorkflowService.getService().add(workflow);

        final WorkflowTriggerService workflowTriggerService = WorkflowTriggerService.getService();
        final WorkflowTrigger dailyTimeWorkflowTrigger = new WorkflowTrigger();
        dailyTimeWorkflowTrigger.setStartAt(System.currentTimeMillis());
        dailyTimeWorkflowTrigger.setEndAt(System.currentTimeMillis() + 100000);
        dailyTimeWorkflowTrigger.setEnabled(true);
        dailyTimeWorkflowTrigger.setWorkflow(workflow.getName());
        dailyTimeWorkflowTrigger.setNamespace(workflow.getNamespace());
        dailyTimeWorkflowTrigger.setName(UUID.randomUUID().toString());
        final DailyTimeIntervalSchedule dailyTimeIntervalSchedule = new DailyTimeIntervalSchedule();
        dailyTimeIntervalSchedule.setRepeatInterval(3);
        dailyTimeIntervalSchedule.setRepeatIntervalUnit(DateBuilder.IntervalUnit.MINUTE);
        dailyTimeIntervalSchedule.setRepeatCount(4);
        dailyTimeIntervalSchedule.setDaysOfWeek(new HashSet<>(Arrays.asList(1, 2, 3, 6)));
        dailyTimeWorkflowTrigger.setSchedule(dailyTimeIntervalSchedule);
        workflowTriggerService.add(dailyTimeWorkflowTrigger);
        final WorkflowTrigger workflowTrigger = workflowTriggerService.get(dailyTimeWorkflowTrigger);
        Assert.assertNotNull(workflowTrigger);
        Assert.assertEquals(dailyTimeWorkflowTrigger, workflowTrigger);
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

        Assert.assertEquals(1, workflowTriggerService.get(namespaceOne.getName(), workflowOne.getName()).size());
        Assert.assertEquals(1, workflowTriggerService.get(namespaceTwo.getName(), workflowTwo.getName()).size());
    }

    @Test(expected = ValidationException.class)
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

    @Test
    public void testWorkflowTriggerPropertiesOne() throws Exception {
        HashMap<String, Object> workflowProps = new HashMap<>();
        workflowProps.put("valOne", 1234);
        workflowProps.put("valTwo", "abcd");

        HashMap<String, Object> triggerProps = new HashMap<>();
        triggerProps.put("valOne", 123456);
        triggerProps.put("valTwo", "abcdef");

        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-with-properties.yaml",
                workflowProps, triggerProps);

        final Scheduler scheduler = WorkflowSchedulerService.getService().getScheduler();
        waitForTriggerToComplete(workflowTrigger, scheduler);
        // wait for tasks status to be consumed from queue
        Thread.sleep(1000);

        TaskService taskService = TaskService.getService();
        final List<Task> workflowTasks = taskService.get(workflowTrigger.getNamespace());
        Assert.assertEquals(3, workflowTasks.size());
        for (Task workflowTask : workflowTasks) {
            Assert.assertEquals(MockSuccessTaskHandler.CONTEXT, workflowTask.getContext());
            if (workflowTask.getName().equals("taskTwo")) {
                Assert.assertEquals(123456, workflowTask.getProperties().get("keyB"));
            }
            if (workflowTask.getName().equals("taskThree")) {
                Assert.assertEquals("abcdef", workflowTask.getProperties().get("keyB"));
            }
        }
    }

    @Test
    public void testWorkflowTriggerPropertiesTwo() throws Exception {
        HashMap<String, Object> workflowProps = new HashMap<>();
        workflowProps.put("valOne", 1234);
        workflowProps.put("valTwo", "abcd");

        HashMap<String, Object> triggerProps = new HashMap<>();
        triggerProps.put("valOne", 123456);

        final WorkflowTrigger workflowTrigger = scheduleWorkflow("workflows/workflow-template-with-properties.yaml",
                workflowProps, triggerProps);

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
                Assert.assertEquals(123456, workflowTask.getProperties().get("keyB"));
            }
            if (workflowTask.getName().equals("taskThree")) {
                Assert.assertEquals("abcd", workflowTask.getProperties().get("keyB"));
            }
        }
    }
}
