package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.TestUtil;
import com.cognitree.kronos.executor.handlers.TestTaskHandler;
import com.cognitree.kronos.model.Task;
import org.junit.Assert;
import org.junit.Test;

import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;

public class ExecutorServiceTest extends ApplicationTest {

    @Test
    public void testMaxParallelTask() {
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().add(taskOne);
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().add(taskTwo);
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().add(taskThree);
        Task taskFour = TestUtil.getTaskBuilder().setName("taskFour").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().add(taskFour);
        Task taskFive = TestUtil.getTaskBuilder().setName("taskFive").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().add(taskFive);
        TestUtil.waitForTaskToFinishExecution(2000);
        Assert.assertEquals(taskOne.getStatus(), RUNNING);
        Assert.assertEquals(taskTwo.getStatus(), RUNNING);
        Assert.assertEquals(taskThree.getStatus(), RUNNING);
        Assert.assertEquals(taskFour.getStatus(), RUNNING);
        Assert.assertEquals(taskFive.getStatus(), SUBMITTED);
        TestTaskHandler.finishExecution(taskOne.getId());
        TestUtil.waitForTaskToFinishExecution(500);
        Assert.assertEquals(taskFive.getStatus(), RUNNING);
        TestTaskHandler.finishExecution(taskTwo.getId());
        TestTaskHandler.finishExecution(taskThree.getId());
        TestTaskHandler.finishExecution(taskFour.getId());
        TestTaskHandler.finishExecution(taskFive.getId());
        TestUtil.waitForTaskToFinishExecution(500);
    }
}

