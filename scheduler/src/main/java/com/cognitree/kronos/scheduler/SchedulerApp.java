package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.scheduler.readers.TaskReaderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * starts the scheduler app by reading configurations from classpath.
 */
public class SchedulerApp {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerApp.class);

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    public static void main(String[] args) {
        try {
            final SchedulerApp schedulerApp = new SchedulerApp();
            Runtime.getRuntime().addShutdownHook(new Thread(schedulerApp::stop));
            schedulerApp.start();
        } catch (Exception e) {
            logger.error("Error starting application", e);
            System.exit(0);
        }
    }

    public void start() throws Exception {
        final InputStream schedulerConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("scheduler.yaml");
        final SchedulerConfig schedulerConfig = MAPPER.readValue(schedulerConfigAsStream, SchedulerConfig.class);

        final InputStream queueConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("queue.yaml");
        final QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);

        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(schedulerConfig, queueConfig);
        TaskReaderService taskReaderService = new TaskReaderService(schedulerConfig.getTaskReaderConfig());

        ServiceProvider.registerService(taskSchedulerService);
        ServiceProvider.registerService(taskReaderService);

        taskSchedulerService.init();
        taskReaderService.init();

        taskSchedulerService.start();
        taskReaderService.start();
    }

    public void stop() {
        if (TaskSchedulerService.getService() != null) {
            TaskSchedulerService.getService().stop();
        }
        if (TaskSchedulerService.getService() != null) {
            TaskSchedulerService.getService().stop();
        }
    }
}

