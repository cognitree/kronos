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

package com.cognitree.kronos;

import com.cognitree.kronos.scheduler.NamespaceService;
import com.cognitree.kronos.scheduler.WorkflowService;
import com.cognitree.kronos.scheduler.WorkflowTriggerService;
import com.cognitree.kronos.scheduler.model.CronSchedule;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Assert;
import org.quartz.CronExpression;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class TestUtil {
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    // needs to be minimum 2 seconds otherwise the workflow is triggered twice
    // as we setting startAt and endAt based on scheduled and quartz convert this to date and time (HH : MM : SS)
    // due to which the precision is lost if set to run every sec.
    private static final CronSchedule SCHEDULE = new CronSchedule();

    static {
        SCHEDULE.setCronExpression("0/2 * * * * ?");
    }

    public static Namespace createNamespace(String name) {
        Namespace namespace = new Namespace();
        namespace.setName(name);
        return namespace;
    }

    public static Workflow createWorkflow(String workflowTemplate, String workflowName,
                                          String namespace) throws IOException {
        return createWorkflow(workflowTemplate, workflowName, namespace, null);
    }

    public static Workflow createWorkflow(String workflowTemplate, String workflowName,
                                          String namespace, Map<String, Object> properties) throws IOException {
        final InputStream resourceAsStream =
                TestUtil.class.getClassLoader().getResourceAsStream(workflowTemplate);
        final Workflow workflow = YAML_MAPPER.readValue(resourceAsStream, Workflow.class);
        workflow.setName(workflowName);
        workflow.setNamespace(namespace);
        workflow.setProperties(properties);
        return workflow;
    }

    public static WorkflowTrigger createWorkflowTrigger(String triggerName, String workflow, String namespace)
            throws ParseException {
        return createWorkflowTrigger(triggerName, workflow, namespace, null);
    }

    public static WorkflowTrigger createWorkflowTrigger(String triggerName, String workflow, String namespace,
                                                        Map<String, Object> properties) throws ParseException {
        final long nextFireTime = new CronExpression(SCHEDULE.getCronExpression())
                .getNextValidTimeAfter(new Date(System.currentTimeMillis() + 100)).getTime();
        final WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName(triggerName);
        workflowTrigger.setWorkflow(workflow);
        workflowTrigger.setNamespace(namespace);
        workflowTrigger.setSchedule(SCHEDULE);
        workflowTrigger.setStartAt(nextFireTime - 100);
        workflowTrigger.setEndAt(nextFireTime + 100);
        workflowTrigger.setProperties(properties);
        return workflowTrigger;
    }

    public static WorkflowTrigger scheduleWorkflow(String workflowTemplate) throws Exception {
        return scheduleWorkflow(workflowTemplate, null, null);
    }

    public static WorkflowTrigger scheduleWorkflow(String workflowTemplate, Map<String, Object> workflowProps,
                                                   Map<String, Object> triggerProps) throws Exception {
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespace);
        final Workflow workflow = createWorkflow(workflowTemplate,
                UUID.randomUUID().toString(), namespace.getName(), workflowProps);
        WorkflowService.getService().add(workflow);
        final WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflow.getName(), workflow.getNamespace(), triggerProps);
        WorkflowTriggerService.getService().add(workflowTrigger);
        return workflowTrigger;
    }

    public static void waitForTriggerToComplete(WorkflowTrigger workflowTriggerOne, Scheduler scheduler) throws Exception {
        // wait for both the job to be triggered
        TriggerKey workflowOneTriggerKey = new TriggerKey(workflowTriggerOne.getName(),
                workflowTriggerOne.getWorkflow() + ":" + workflowTriggerOne.getNamespace());
        int maxCount = 50;
        while (maxCount > 0 && scheduler.checkExists(workflowOneTriggerKey)) {
            Thread.sleep(100);
            maxCount--;
        }
        if (maxCount < 0) {
            Assert.fail("failed while waiting for trigger to complete");
        }
    }
}
