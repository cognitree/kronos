package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ApplicationConfig;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.TaskDefinition;
import com.cognitree.kronos.scheduler.readers.MockTaskDefinitionReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.io.InputStream;

import static com.cognitree.kronos.TestUtil.sleep;

public class TaskReaderServiceTest {

    @BeforeClass
    public static void init() throws Exception {
        InputStream appResourceStream = TaskReaderServiceTest.class.getClassLoader().getResourceAsStream("app.yaml");
        ApplicationConfig applicationConfig =
                new ObjectMapper(new YAMLFactory()).readValue(appResourceStream, ApplicationConfig.class);

        TaskReaderService taskReaderService = new TaskReaderService(applicationConfig.getReaderConfig());
        ServiceProvider.registerService(taskReaderService);
        taskReaderService.init();
        taskReaderService.start();

        TaskSchedulerService taskSchedulerService =
                new TaskSchedulerService(applicationConfig.getProducerConfig(), applicationConfig.getConsumerConfig(),
                        applicationConfig.getHandlerConfig(), applicationConfig.getTimeoutPolicyConfig(),
                        applicationConfig.getTaskStoreConfig(), applicationConfig.getTaskPurgeInterval());
        ServiceProvider.registerService(taskSchedulerService);
        taskSchedulerService.init();
        taskSchedulerService.start();
    }

    @AfterClass
    public static void cleanup() {
        ServiceProvider.getTaskReaderService().stop();
        ServiceProvider.getTaskSchedulerService().stop();
    }

    @Test
    public void testScheduledTaskDefinitionReaderJob() throws SchedulerException {
        final Scheduler scheduler = ServiceProvider.getTaskReaderService().getScheduler();
        final JobKey mockReaderJobKey = new JobKey("mockReader", "mockReaderjobScheduler");
        Assert.assertTrue(scheduler.checkExists(mockReaderJobKey));

        final JobKey testReaderJobKey = new JobKey("testReader", "testReaderjobScheduler");
        Assert.assertFalse(scheduler.checkExists(testReaderJobKey));

    }

    @Test
    public void testScheduledTaskDefinitionJob() throws SchedulerException {
        final Scheduler scheduler = ServiceProvider.getTaskReaderService().getScheduler();
        sleep(1000);

        final JobKey taskOneDefinitionReaderJobKey =
                new JobKey("default:taskOne:test", "mockReaderGroup");
        Assert.assertTrue(scheduler.checkExists(taskOneDefinitionReaderJobKey));
        Assert.assertEquals(MockTaskDefinitionReader.getTaskDefinition("taskOne"),
                scheduler.getJobDetail(taskOneDefinitionReaderJobKey).getJobDataMap().get("taskDefinition"));

        final JobKey taskTwoDefinitionReaderJobKey =
                new JobKey("default:taskTwo:test", "mockReaderGroup");
        Assert.assertTrue(scheduler.checkExists(taskTwoDefinitionReaderJobKey));
        Assert.assertEquals(MockTaskDefinitionReader.getTaskDefinition("taskTwo"),
                scheduler.getJobDetail(taskTwoDefinitionReaderJobKey).getJobDataMap().get("taskDefinition"));

        MockTaskDefinitionReader.updateTaskDefinition("taskOne", "0/1 * * 1/1 * ? *");
        MockTaskDefinitionReader.removeTaskDefinition("taskTwo");
        sleep(1000);
        Assert.assertTrue(scheduler.checkExists(taskOneDefinitionReaderJobKey));
        final TaskDefinition taskOne = MockTaskDefinitionReader.getTaskDefinition("taskOne");
        System.out.println(taskOne);
        final Object taskDefinition = scheduler.getJobDetail(taskOneDefinitionReaderJobKey).getJobDataMap().get("taskDefinition");
        System.out.println(taskDefinition);
        Assert.assertEquals(taskOne,
                taskDefinition);
        Assert.assertFalse(scheduler.checkExists(taskTwoDefinitionReaderJobKey));

        final TaskProvider taskProvider = ServiceProvider.getTaskSchedulerService().getTaskProvider();
        int taskCount = taskProvider.size();
        sleep(1000);
        Assert.assertEquals(++taskCount, taskProvider.size());
        sleep(1000);
        Assert.assertEquals(++taskCount, taskProvider.size());
        sleep(1000);
        Assert.assertEquals(++taskCount, taskProvider.size());
    }
}
