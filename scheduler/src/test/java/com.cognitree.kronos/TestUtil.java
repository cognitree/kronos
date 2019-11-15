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

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.JobService;
import com.cognitree.kronos.scheduler.NamespaceService;
import com.cognitree.kronos.scheduler.TaskService;
import com.cognitree.kronos.scheduler.ValidationException;
import com.cognitree.kronos.scheduler.WorkflowService;
import com.cognitree.kronos.scheduler.WorkflowTriggerService;
import com.cognitree.kronos.scheduler.model.CronSchedule;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.events.ConfigUpdate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.quartz.CronExpression;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.cognitree.kronos.model.Task.Status.RUNNING;

public class TestUtil {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private static final CronSchedule SCHEDULE = new CronSchedule();

    static {
        SCHEDULE.setCronExpression("0/2 * * * * ?");
    }

    public static ConfigUpdate createConfigUpdate(ConfigUpdate.Action action, Object model) {
        ConfigUpdate configUpdate = new ConfigUpdate();
        configUpdate.setAction(action);
        configUpdate.setModel(model);
        return configUpdate;
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

    public static WorkflowTrigger createWorkflowTrigger(String triggerName, String workflow, String namespace) throws ParseException {
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
        workflowTrigger.setProperties(properties);
        workflowTrigger.setStartAt(nextFireTime - 100);
        workflowTrigger.setEndAt(nextFireTime + 100);
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

    public static void waitForJobsToTriggerAndComplete(WorkflowTrigger workflowTrigger) throws Exception {
        int maxCount = 180;
        while (maxCount > 0) {
            final List<Job> jobs = JobService.getService().get(workflowTrigger.getNamespace(), workflowTrigger.getWorkflow(),
                    workflowTrigger.getName(), 0, System.currentTimeMillis());
            if (jobs.size() > 0) {
                Optional<Job> optionalJob = jobs.stream().filter(job -> !job.getStatus().isFinal()).findFirst();
                if (!optionalJob.isPresent()) {
                    break;
                }
            }
            Thread.sleep(1000);
            maxCount--;
        }
    }

    public static void waitForTriggerToComplete(WorkflowTrigger workflowTrigger, Scheduler scheduler) throws Exception {
        // wait for both the job to be triggered
        TriggerKey workflowOneTriggerKey = new TriggerKey(workflowTrigger.getName(),
                workflowTrigger.getWorkflow() + ":" + workflowTrigger.getNamespace());
        int maxCount = 120;
        while (maxCount > 0 && scheduler.checkExists(workflowOneTriggerKey)) {
            Thread.sleep(1000);
            maxCount--;
        }
    }

    public static void waitForTaskToBeRunning(Task task) throws ServiceException, ValidationException, InterruptedException {
        int maxCount = 120;
        while (maxCount > 0) {
            if (TaskService.getService().get(task).getStatus() == RUNNING) {
                break;
            }
            Thread.sleep(1000);
            maxCount--;
        }
    }
}
