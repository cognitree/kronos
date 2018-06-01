package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.TestUtil;
import com.cognitree.kronos.executor.handlers.TestTaskHandler;
import com.cognitree.kronos.executor.handlers.TypeATaskHandler;
import com.cognitree.kronos.executor.handlers.TypeBTaskHandler;
import com.cognitree.kronos.model.Task;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static com.cognitree.kronos.model.Task.Status.*;

@FixMethodOrder(MethodSorters.JVM)
public class TaskExecutorServiceTest extends ApplicationTest {

    @Test
    public void testMaxParallelTask() {
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskOne);
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskTwo);
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskThree);
        Task taskFour = TestUtil.getTaskBuilder().setName("taskFour").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskFour);
        Task taskFive = TestUtil.getTaskBuilder().setName("taskFive").waitForCallback(true).setType("test").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskFive);
        TestUtil.waitForTaskToFinishExecution(1000);
        // TODO what to check here status can be running or submitted depending on executor
        Assert.assertEquals(RUNNING, taskOne.getStatus());
        Assert.assertEquals(RUNNING, taskTwo.getStatus());
        Assert.assertEquals(RUNNING, taskThree.getStatus());
        Assert.assertEquals(RUNNING, taskFour.getStatus());
        Assert.assertEquals(SCHEDULED, taskFive.getStatus());
        TestTaskHandler.finishExecution(taskOne.getId());
        TestUtil.waitForTaskToFinishExecution(500);
        Assert.assertEquals(SUCCESSFUL, taskOne.getStatus());
        Assert.assertEquals(RUNNING, taskFive.getStatus());
        TestTaskHandler.finishExecution(taskTwo.getId());
        TestTaskHandler.finishExecution(taskThree.getId());
        TestTaskHandler.finishExecution(taskFour.getId());
        TestTaskHandler.finishExecution(taskFive.getId());
        TestUtil.waitForTaskToFinishExecution(500);
        Assert.assertEquals(SUCCESSFUL, taskTwo.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskThree.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskFour.getStatus());
        Assert.assertEquals(SUCCESSFUL, taskFive.getStatus());
    }

    @Test
    public void testTaskToHandlerMapping() {
        Task taskOne = TestUtil.getTaskBuilder().setName("taskOne").setType("typeA").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskOne);
        Task taskTwo = TestUtil.getTaskBuilder().setName("taskTwo").setType("typeB").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskTwo);
        Task taskThree = TestUtil.getTaskBuilder().setName("taskThree").setType("typeA").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskThree);
        Task taskFour = TestUtil.getTaskBuilder().setName("taskFour").setType("typeA").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskFour);
        Task taskFive = TestUtil.getTaskBuilder().setName("taskFive").setType("typeB").build();
        ServiceProvider.getTaskSchedulerService().schedule(taskFive);
        TestUtil.waitForTaskToFinishExecution(2000);
        Assert.assertTrue(TypeATaskHandler.isHandled(taskOne.getId()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskOne.getId()));
        Assert.assertTrue(TypeBTaskHandler.isHandled(taskTwo.getId()));
        Assert.assertFalse(TypeATaskHandler.isHandled(taskTwo.getId()));
        Assert.assertTrue(TypeATaskHandler.isHandled(taskThree.getId()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskThree.getId()));
        Assert.assertTrue(TypeATaskHandler.isHandled(taskFour.getId()));
        Assert.assertFalse(TypeBTaskHandler.isHandled(taskFour.getId()));
        Assert.assertTrue(TypeBTaskHandler.isHandled(taskFive.getId()));
        Assert.assertFalse(TypeATaskHandler.isHandled(taskFive.getId()));
    }
}

