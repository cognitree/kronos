package com.cognitree.kronos;

import com.cognitree.kronos.scheduler.JobService;
import com.cognitree.kronos.scheduler.ServiceTest;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.store.StoreService;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.cognitree.kronos.TestUtil.scheduleWorkflow;
import static com.cognitree.kronos.TestUtil.waitForJobsToTriggerAndComplete;

public class ApplicationTest extends ServiceTest {

    @Test
    public void testSchedulerDown() throws Exception {
        final StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        if (!storeService.isPersistent()) {
            return;
        }
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        SCHEDULER_APP.stop();
        Thread.sleep(1000);
        SCHEDULER_APP.start();

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace());
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));
        Assert.assertFalse(workflowOneJobs.stream().anyMatch(t -> t.getStatus() != Job.Status.SUCCESSFUL));

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace());
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
    }


    @Test
    public void testExecutorDown() throws Exception {
        final StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        if (!storeService.isPersistent()) {
            return;
        }
        EXECUTOR_APP.stop();
        final WorkflowTrigger workflowTriggerOne = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        final WorkflowTrigger workflowTriggerTwo = scheduleWorkflow(WORKFLOW_TEMPLATE_YAML);
        Thread.sleep(1000);
        EXECUTOR_APP.start();

        waitForJobsToTriggerAndComplete(workflowTriggerOne);
        waitForJobsToTriggerAndComplete(workflowTriggerTwo);

        JobService jobService = JobService.getService();
        final List<Job> workflowOneJobs = jobService.get(workflowTriggerOne.getNamespace());
        Assert.assertEquals(1, workflowOneJobs.size());
        Assert.assertNotNull(jobService.get(workflowOneJobs.get(0)));

        final List<Job> workflowTwoJobs = jobService.get(workflowTriggerTwo.getNamespace());
        Assert.assertEquals(1, workflowTwoJobs.size());
        Assert.assertNotNull(jobService.get(workflowTwoJobs.get(0)));
    }
}
