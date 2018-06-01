package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ApplicationConfig;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.executor.TaskExecutionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.InputStream;

public class ApplicationTest {
    static ApplicationConfig applicationConfig;
    static TaskProvider taskProvider;

    @BeforeClass
    public static void init() throws Exception {
        InputStream appResourceStream = TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("app.yaml");
        applicationConfig = new ObjectMapper(new YAMLFactory()).readValue(appResourceStream, ApplicationConfig.class);
        initTaskSchedulerService();
        initTaskExecutionService();
    }

    @SuppressWarnings("unchecked")
    private static void initTaskSchedulerService() throws Exception {
        TaskSchedulerService taskSchedulerService =
                new TaskSchedulerService(applicationConfig.getProducerConfig(), applicationConfig.getConsumerConfig(),
                        applicationConfig.getHandlerConfig(), applicationConfig.getTimeoutPolicyConfig(),
                        applicationConfig.getTaskStoreConfig(), applicationConfig.getTaskPurgeInterval());
        ServiceProvider.registerService(taskSchedulerService);
        taskSchedulerService.init();
        taskSchedulerService.start();
        taskProvider = ServiceProvider.getTaskSchedulerService().getTaskProvider();
    }

    @SuppressWarnings("unchecked")
    private static void initTaskExecutionService() throws Exception {
        TaskExecutionService taskExecutionService =
                new TaskExecutionService(applicationConfig.getConsumerConfig(), applicationConfig.getProducerConfig(),
                        applicationConfig.getHandlerConfig());
        ServiceProvider.registerService(taskExecutionService);
        taskExecutionService.init();
        taskExecutionService.start();
    }

    @AfterClass
    public static void stop() {
        ServiceProvider.getTaskExecutionService().stop();
        ServiceProvider.getTaskSchedulerService().stop();
    }

    @Before
    public void initialize() {
        // reinit will clear all the tasks from task provider
        ServiceProvider.getTaskSchedulerService().reinitTaskProvider();
    }
}
