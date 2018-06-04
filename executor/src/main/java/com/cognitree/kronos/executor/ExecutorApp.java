package com.cognitree.kronos.executor;

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.queue.QueueConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * starts the executor app by reading configurations from classpath.
 */
public class ExecutorApp {

    private static final Logger logger = LoggerFactory.getLogger(ExecutorApp.class);

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    public static void main(String[] args) {
        try {
            final ExecutorApp executorApp = new ExecutorApp();
            Runtime.getRuntime().addShutdownHook(new Thread(executorApp::stop));
            executorApp.start();
        } catch (Exception e) {
            logger.error("Error starting application", e);
            System.exit(0);
        }
    }

    public void start() throws Exception {
        final InputStream executorConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("executor.yaml");
        final ExecutorConfig executorConfig = MAPPER.readValue(executorConfigAsStream, ExecutorConfig.class);

        final InputStream queueConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("queue.yaml");
        final QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);

        TaskExecutionService taskExecutionService = new TaskExecutionService(executorConfig, queueConfig);
        ServiceProvider.registerService(taskExecutionService);
        taskExecutionService.init();
        taskExecutionService.start();
    }

    public void stop() {
        if (TaskExecutionService.getService() != null) {
            TaskExecutionService.getService().stop();
        }
    }
}


