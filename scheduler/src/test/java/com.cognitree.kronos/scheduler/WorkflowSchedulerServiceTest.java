package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.InputStream;
import java.util.Date;
import java.util.List;

import static java.lang.Thread.sleep;

@FixMethodOrder(MethodSorters.JVM)
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
        WorkflowSchedulerService.getService().validate(workflowDefinition);
    }

    @Test(expected = ValidationException.class)
    public void testInValidWorkflowMissingSchedule() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("invalid-workflow-missing-schedule.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowSchedulerService.getService().validate(workflowDefinition);
    }

    @Test(expected = ValidationException.class)
    public void testInValidWorkflowMissingTasks() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("invalid-workflow-missing-tasks.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowSchedulerService.getService().validate(workflowDefinition);
    }

    @Test(expected = ValidationException.class)
    public void testInValidWorkflowDisabledTaskDependency() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("invalid-workflow-disabled-tasks-dependency.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowSchedulerService.getService().validate(workflowDefinition);
    }

    @Test
    public void testResolveWorkflowTasks() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("valid-workflow.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowSchedulerService.getService().validate(workflowDefinition);
        final List<WorkflowDefinition.WorkflowTask> workflowTasks =
                WorkflowSchedulerService.getService().resolveWorkflowTasks(workflowDefinition.getTasks());
        Assert.assertEquals("task one", workflowTasks.get(0).getName());
        Assert.assertEquals("task two", workflowTasks.get(1).getName());
        Assert.assertEquals("task three", workflowTasks.get(2).getName());
    }

    @Test
    public void testExecuteWorkflow() throws Exception {
        final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream("test-workflow.yaml");
        final WorkflowDefinition workflowDefinition = MAPPER.readValue(resourceAsStream, WorkflowDefinition.class);
        WorkflowSchedulerService.getService().validate(workflowDefinition);
        final List<WorkflowDefinition.WorkflowTask> workflowTasks =
                WorkflowSchedulerService.getService().resolveWorkflowTasks(workflowDefinition.getTasks());
        Assert.assertEquals(0, TaskSchedulerService.getService().getTaskProvider().size());
        WorkflowSchedulerService.getService().execute(workflowDefinition.getName(), workflowDefinition.getNamespace(),
                workflowTasks, new Date(System.currentTimeMillis() + 5));
        sleep(100);
        Assert.assertEquals(3, TaskSchedulerService.getService().getTaskProvider().size());
    }
}
