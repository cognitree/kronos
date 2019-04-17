package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.scheduler.events.ConfigUpdate;
import com.cognitree.kronos.scheduler.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.quartz.CronExpression;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;

class TestHelper {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private static final CronSchedule schedule = new CronSchedule();

    static {
        schedule.setCronExpression("0/2 * * * * ?");
    }

    static ConfigUpdate createConfigUpdate(ConfigUpdate.Action action, Object model) {
        ConfigUpdate configUpdate = new ConfigUpdate();
        configUpdate.setAction(action);
        configUpdate.setModel(model);
        return configUpdate;
    }

    static Namespace createNamespace(String nsName) {
        Namespace namespace = new Namespace();
        namespace.setName(nsName);
        namespace.setDescription("testDescription");
        return namespace;
    }

    static Workflow createWorkflow(String workflowName, String namespace) throws IOException {
        final InputStream resourceAsStream =
                TestHelper.class.getClassLoader().getResourceAsStream("workflow.yaml");
        final Workflow workflow = YAML_MAPPER.readValue(resourceAsStream, Workflow.class);
        workflow.setName(workflowName);
        workflow.setNamespace(namespace);
        return workflow;
    }

    static WorkflowTrigger createWorkflowTrigger(String triggerName, String workflow, String namespace) throws ParseException {
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

    static WorkflowTrigger createSimpleWorkflowTrigger(String triggerName, String workflow, String namespace) throws ParseException {
        final WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName(triggerName);
        workflowTrigger.setWorkflow(workflow);
        workflowTrigger.setNamespace(namespace);
        SimpleSchedule schedule = new SimpleSchedule();
        schedule.setRepeatForever(true);
        schedule.setRepeatIntervalInMs(1000);
        workflowTrigger.setSchedule(schedule);
        return workflowTrigger;
    }
}
