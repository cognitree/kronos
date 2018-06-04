/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos;

import com.cognitree.kronos.executor.ExecutorConfig;
import com.cognitree.kronos.executor.TaskExecutionService;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.scheduler.SchedulerConfig;
import com.cognitree.kronos.scheduler.TaskReaderService;
import com.cognitree.kronos.scheduler.TaskSchedulerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Main class which instantiates and starts services required by the scheduler framework
 * <p>
 * The started services are registered with the {@link ServiceProvider} for future reference
 * </p>
 */
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final String DEFAULT_PROFILE = "all";
    private static final String SCHEDULER_PROFILE = "scheduler";
    private static final String EXECUTOR_PROFILE = "executor";
    private static final String PROFILE = System.getProperty("profile", DEFAULT_PROFILE);

    public static void main(String[] args) {
        final Application application = new Application();
        Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
        try {
            logger.info("Starting application with profile: {}", PROFILE);
            application.register();
            application.init();
            application.start();
        } catch (Exception e) {
            logger.error("Error starting application", e);
            System.exit(0);
        }
    }

    private void register() throws IOException {
        switch (PROFILE) {
            case SCHEDULER_PROFILE:
                registerScheduler();
                break;
            case EXECUTOR_PROFILE:
                registerExecutor();
                break;
            case DEFAULT_PROFILE:
                registerScheduler();
                registerExecutor();
        }
    }

    private void registerScheduler() throws IOException {
        final InputStream schedulerConfigAsStream =
                Application.class.getClassLoader().getResourceAsStream("scheduler.yaml");
        final SchedulerConfig schedulerConfig = MAPPER.readValue(schedulerConfigAsStream, SchedulerConfig.class);

        final InputStream queueConfigAsStream =
                Application.class.getClassLoader().getResourceAsStream("queue.yaml");
        final QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);

        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(schedulerConfig, queueConfig);
        ServiceProvider.registerService(taskSchedulerService);

        TaskReaderService taskReaderService = new TaskReaderService(schedulerConfig.getTaskReaderConfig());
        ServiceProvider.registerService(taskReaderService);
    }

    private void registerExecutor() throws IOException {
        final InputStream executorConfigAsStream =
                Application.class.getClassLoader().getResourceAsStream("executor.yaml");
        final ExecutorConfig executorConfig = MAPPER.readValue(executorConfigAsStream, ExecutorConfig.class);

        final InputStream queueConfigAsStream =
                Application.class.getClassLoader().getResourceAsStream("queue.yaml");
        final QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);

        TaskExecutionService taskExecutionService = new TaskExecutionService(executorConfig, queueConfig);
        ServiceProvider.registerService(taskExecutionService);
    }

    public void init() throws Exception {
        switch (PROFILE) {
            case SCHEDULER_PROFILE:
                initScheduler();
                break;
            case EXECUTOR_PROFILE:
                initExecutor();
                break;
            case DEFAULT_PROFILE:
                initScheduler();
                initExecutor();
        }
    }

    private void initScheduler() throws Exception {
        ServiceProvider.getTaskReaderService().init();
        ServiceProvider.getTaskSchedulerService().init();
    }

    private void initExecutor() throws Exception {
        ServiceProvider.getTaskExecutionService().init();
    }

    public void start() throws SchedulerException {
        switch (PROFILE) {
            case SCHEDULER_PROFILE:
                startScheduler();
                break;
            case EXECUTOR_PROFILE:
                startExecutor();
                break;
            case DEFAULT_PROFILE:
                startScheduler();
                startExecutor();
        }
    }

    private void startScheduler() throws SchedulerException {
        ServiceProvider.getTaskReaderService().start();
        ServiceProvider.getTaskSchedulerService().start();
    }

    private void startExecutor() {
        ServiceProvider.getTaskExecutionService().start();
    }

    public void stop() {
        switch (PROFILE) {
            case SCHEDULER_PROFILE:
                stopScheduler();
                break;
            case EXECUTOR_PROFILE:
                stopExecutor();
                break;
            case DEFAULT_PROFILE:
                stopScheduler();
                stopExecutor();
                break;
        }
    }

    private void stopScheduler() {
        if (ServiceProvider.getTaskReaderService() != null) {
            ServiceProvider.getTaskReaderService().stop();
        }
        if (ServiceProvider.getTaskSchedulerService() != null) {
            ServiceProvider.getTaskSchedulerService().stop();
        }
    }

    private void stopExecutor() {
        if (ServiceProvider.getTaskExecutionService() != null) {
            ServiceProvider.getTaskExecutionService().stop();
        }
    }
}
