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

import com.cognitree.kronos.executor.TaskExecutionService;
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
public class Application implements ComponentLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static final String DEFAULT_PROFILE = "all";
    private static final String SCHEDULER_PROFILE = "scheduler";
    private static final String EXECUTOR_PROFILE = "executor";
    private static final String PROFILE = System.getProperty("profile", DEFAULT_PROFILE);
    private static final String APP_CONFIG_FILE = System.getProperty("configFile", "app.yaml");

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

    private void register() throws Exception {
        InputStream resourceAsStream = Application.class.getClassLoader().getResourceAsStream(APP_CONFIG_FILE);
        if (resourceAsStream == null) {
            throw new IOException("Resource not found: " + APP_CONFIG_FILE);
        }

        ApplicationConfig applicationConfig =
                new ObjectMapper(new YAMLFactory()).readValue(resourceAsStream, ApplicationConfig.class);

        switch (PROFILE) {
            case EXECUTOR_PROFILE:
                registerTaskExecutionService(applicationConfig);
                break;
            case SCHEDULER_PROFILE:
                registerTaskReaderService(applicationConfig);
                registerTaskProviderService(applicationConfig);
                break;
            case DEFAULT_PROFILE:
                registerTaskExecutionService(applicationConfig);
                registerTaskReaderService(applicationConfig);
                registerTaskProviderService(applicationConfig);
        }
    }

    private void registerTaskReaderService(ApplicationConfig applicationConfig) {
        TaskReaderService taskReaderService = new TaskReaderService(applicationConfig.getReaderConfig());
        ServiceProvider.registerService(taskReaderService);
    }

    @SuppressWarnings("unchecked")
    private void registerTaskProviderService(ApplicationConfig applicationConfig) {
        TaskSchedulerService taskSchedulerService =
                new TaskSchedulerService(applicationConfig.getProducerConfig(), applicationConfig.getConsumerConfig(),
                        applicationConfig.getHandlerConfig(), applicationConfig.getTimeoutPolicyConfig(),
                        applicationConfig.getTaskStoreConfig(), applicationConfig.getTaskPurgeInterval());
        ServiceProvider.registerService(taskSchedulerService);
    }

    @SuppressWarnings("unchecked")
    private void registerTaskExecutionService(ApplicationConfig applicationConfig) {
        TaskExecutionService taskExecutionService =
                new TaskExecutionService(applicationConfig.getConsumerConfig(), applicationConfig.getProducerConfig(),
                        applicationConfig.getHandlerConfig());
        ServiceProvider.registerService(taskExecutionService);
    }

    @Override
    public void init() throws Exception {
        switch (PROFILE) {
            case EXECUTOR_PROFILE:
                ServiceProvider.getTaskExecutionService().init();
                break;
            case SCHEDULER_PROFILE:
                ServiceProvider.getTaskReaderService().init();
                ServiceProvider.getTaskSchedulerService().init();
                break;
            case DEFAULT_PROFILE:
                ServiceProvider.getTaskReaderService().init();
                ServiceProvider.getTaskSchedulerService().init();
                ServiceProvider.getTaskExecutionService().init();
        }
    }

    @Override
    public void start() throws SchedulerException {
        switch (PROFILE) {
            case EXECUTOR_PROFILE:
                ServiceProvider.getTaskExecutionService().start();
                break;
            case SCHEDULER_PROFILE:
                ServiceProvider.getTaskReaderService().start();
                ServiceProvider.getTaskSchedulerService().start();
                break;
            case DEFAULT_PROFILE:
                ServiceProvider.getTaskReaderService().start();
                ServiceProvider.getTaskSchedulerService().start();
                ServiceProvider.getTaskExecutionService().start();
        }
    }

    @Override
    public void stop() {
        switch (PROFILE) {
            case SCHEDULER_PROFILE:
                if (ServiceProvider.getTaskReaderService() != null) {
                    ServiceProvider.getTaskReaderService().stop();
                }
                if (ServiceProvider.getTaskSchedulerService() != null) {
                    ServiceProvider.getTaskSchedulerService().stop();
                }
                break;
            case EXECUTOR_PROFILE:
                if (ServiceProvider.getTaskExecutionService() != null) {
                    ServiceProvider.getTaskExecutionService().stop();
                }
                break;
            case DEFAULT_PROFILE:
                if (ServiceProvider.getTaskReaderService() != null) {
                    ServiceProvider.getTaskReaderService().stop();
                }
                if (ServiceProvider.getTaskSchedulerService() != null) {
                    ServiceProvider.getTaskSchedulerService().stop();
                }
                if (ServiceProvider.getTaskExecutionService() != null) {
                    ServiceProvider.getTaskExecutionService().stop();
                }
                break;
        }
    }
}
