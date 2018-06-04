package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.executor.ExecutorConfig;
import com.cognitree.kronos.executor.TaskExecutionService;
import com.cognitree.kronos.queue.QueueConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.InputStream;

public class ApplicationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    static SchedulerConfig schedulerConfig;
    static TaskProvider taskProvider;

    @BeforeClass
    public static void init() throws Exception {
        initTaskSchedulerService();
        initTaskExecutionService();
    }

    @SuppressWarnings("unchecked")
    private static void initTaskSchedulerService() throws Exception {
        InputStream schedulerConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("scheduler.yaml");
        schedulerConfig = MAPPER.readValue(schedulerConfigStream, SchedulerConfig.class);

        InputStream queueConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigStream, QueueConfig.class);

        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(schedulerConfig, queueConfig);
        ServiceProvider.registerService(taskSchedulerService);
        taskSchedulerService.init();
        taskSchedulerService.start();
        taskProvider = TaskSchedulerService.getService().getTaskProvider();
    }

    @SuppressWarnings("unchecked")
    private static void initTaskExecutionService() throws Exception {
        InputStream executorConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("executor.yaml");
        ExecutorConfig executorConfig = MAPPER.readValue(executorConfigStream, ExecutorConfig.class);

        InputStream queueConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigStream, QueueConfig.class);

        TaskExecutionService taskExecutionService =
                new TaskExecutionService(executorConfig, queueConfig);
        ServiceProvider.registerService(taskExecutionService);
        taskExecutionService.init();
        taskExecutionService.start();
    }

    @AfterClass
    public static void stop() {
        TaskExecutionService.getService().stop();
        TaskSchedulerService.getService().stop();
    }

    @Before
    public void initialize() {
        // reinit will clear all the tasks from task provider
        TaskSchedulerService.getService().reinitTaskProvider();
    }
}
