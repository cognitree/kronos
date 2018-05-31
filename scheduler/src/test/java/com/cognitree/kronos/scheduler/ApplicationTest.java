package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ApplicationConfig;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.executor.TaskExecutionService;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskStatus;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.queue.producer.ProducerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
        InputStream appResourceStream = SchedulerServiceTest.class.getClassLoader().getResourceAsStream("app.yaml");
        applicationConfig = new ObjectMapper(new YAMLFactory()).readValue(appResourceStream, ApplicationConfig.class);
        initTaskSchedulerService();
        initTaskExecutionService();
    }

    @SuppressWarnings("unchecked")
    private static void initTaskSchedulerService() throws Exception {
        final ProducerConfig taskProducerConfig = applicationConfig.getTaskProducerConfig();
        Producer<Task> taskProducer = (Producer<Task>) Class.forName(taskProducerConfig.getProducerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(taskProducerConfig.getConfig());

        final ConsumerConfig statusConsumerConfig = applicationConfig.getTaskStatusConsumerConfig();
        Consumer<TaskStatus> statusConsumer = (Consumer<TaskStatus>) Class.forName(statusConsumerConfig.getConsumerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(statusConsumerConfig.getConfig());

        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(taskProducer, statusConsumer,
                applicationConfig.getHandlerConfig(), applicationConfig.getTimeoutPolicyConfig(),
                applicationConfig.getTaskStoreConfig(), applicationConfig.getTaskPurgeInterval());
        ServiceProvider.registerService(taskSchedulerService);
        taskSchedulerService.init();
        taskSchedulerService.start();
        taskProvider = ServiceProvider.getTaskSchedulerService().getTaskProvider();
    }

    @SuppressWarnings("unchecked")
    private static void initTaskExecutionService() throws InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        final ProducerConfig statusProducerConfig = applicationConfig.getTaskStatusProducerConfig();
        Producer<TaskStatus> statusProducer = (Producer<TaskStatus>) Class.forName(statusProducerConfig.getProducerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(statusProducerConfig.getConfig());

        final ConsumerConfig taskConsumerConfig = applicationConfig.getTaskConsumerConfig();
        Consumer<Task> taskConsumer = (Consumer<Task>) Class.forName(taskConsumerConfig.getConsumerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(taskConsumerConfig.getConfig());

        TaskExecutionService taskExecutionService = new TaskExecutionService(taskConsumer, statusProducer,
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
