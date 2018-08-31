package com.cognitree.kronos;

import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.NamespaceService;
import com.cognitree.kronos.scheduler.SchedulerApp;
import com.cognitree.kronos.scheduler.WorkflowService;
import com.cognitree.kronos.scheduler.WorkflowTriggerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.quartz.CronExpression;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.UUID;

public class ApplicationTest {
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    // needs to be minimum 2 seconds otherwise the workflow is triggered twice
    // as we setting startAt and endAt based on scheduled and quartz convert this to date and time (HH : MM : SS)
    // due to which the precision is lost if set to run every sec.
    private static final String SCHEDULE = "0/2 * * * * ?";

    @BeforeClass
    public static void setup() throws Exception {
        new SchedulerApp().start();
        new ExecutorApp().start();
    }

    @AfterClass
    public static void destroy() {
        new SchedulerApp().stop();
        new ExecutorApp().stop();
    }

    protected Namespace createNamespace(String name) {
        Namespace namespace = new Namespace();
        namespace.setName(name);
        return namespace;
    }

    protected Workflow createWorkflow(String workflowTemplate, String workflowName,
                                      String namespace) throws IOException {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream(workflowTemplate);
        final Workflow workflow = YAML_MAPPER.readValue(resourceAsStream, Workflow.class);
        workflow.setName(workflowName);
        workflow.setNamespace(namespace);
        return workflow;
    }

    protected WorkflowTrigger createWorkflowTrigger(String triggerName, String workflow, String namespace) throws ParseException {
        final long nextFireTime = new CronExpression(SCHEDULE)
                .getNextValidTimeAfter(new Date(System.currentTimeMillis() + 100)).getTime();
        final WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName(triggerName);
        workflowTrigger.setWorkflow(workflow);
        workflowTrigger.setNamespace(namespace);
        workflowTrigger.setSchedule(SCHEDULE);
        workflowTrigger.setStartAt(nextFireTime - 100);
        workflowTrigger.setEndAt(nextFireTime + 100);
        System.out.println(workflowTrigger);
        return workflowTrigger;
    }

    protected WorkflowTrigger scheduleWorkflow(String workflowTemplate) throws Exception {
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        NamespaceService.getService().add(namespace);
        final Workflow workflow = createWorkflow(workflowTemplate,
                UUID.randomUUID().toString(), namespace.getName());
        WorkflowService.getService().add(workflow);
        final WorkflowTrigger workflowTrigger = createWorkflowTrigger(UUID.randomUUID().toString(),
                workflow.getName(), workflow.getNamespace());
        WorkflowTriggerService.getService().add(workflowTrigger);
        return workflowTrigger;
    }

    protected void waitForTriggerToComplete(WorkflowTrigger workflowTriggerOne, Scheduler scheduler) throws Exception {
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
