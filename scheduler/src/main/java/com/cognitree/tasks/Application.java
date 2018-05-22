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

package com.cognitree.tasks;

import com.cognitree.tasks.executor.TaskExecutionService;
import com.cognitree.tasks.model.Task;
import com.cognitree.tasks.model.TaskStatus;
import com.cognitree.tasks.queue.consumer.Consumer;
import com.cognitree.tasks.queue.consumer.ConsumerConfig;
import com.cognitree.tasks.queue.producer.Producer;
import com.cognitree.tasks.queue.producer.ProducerConfig;
import com.cognitree.tasks.scheduler.TaskProviderService;
import com.cognitree.tasks.scheduler.TaskReaderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    private void registerTaskProviderService(ApplicationConfig applicationConfig) throws Exception {
        final ProducerConfig taskProducerConfig = applicationConfig.getTaskProducerConfig();
        Producer<Task> taskProducer = (Producer<Task>) Class.forName(taskProducerConfig.getProducerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(taskProducerConfig.getConfig());

        final ConsumerConfig statusConsumerConfig = applicationConfig.getTaskStatusConsumerConfig();
        Consumer<TaskStatus> statusConsumer = (Consumer<TaskStatus>) Class.forName(statusConsumerConfig.getConsumerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(statusConsumerConfig.getConfig());

        TaskProviderService taskProviderService = new TaskProviderService(taskProducer, statusConsumer, applicationConfig.getHandlerConfig(),
                applicationConfig.getTimeoutPolicyConfig(), applicationConfig.getTaskStoreConfig(), applicationConfig.getTaskPurgeInterval());
        ServiceProvider.registerService(taskProviderService);
    }

    @SuppressWarnings("unchecked")
    private void registerTaskExecutionService(ApplicationConfig applicationConfig) throws Exception {
        final ProducerConfig statusProducerConfig = applicationConfig.getTaskStatusProducerConfig();
        Producer<TaskStatus> statusProducer = (Producer<TaskStatus>) Class.forName(statusProducerConfig.getProducerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(statusProducerConfig.getConfig());

        final ConsumerConfig taskConsumerConfig = applicationConfig.getTaskConsumerConfig();
        Consumer<Task> taskConsumer = (Consumer<Task>) Class.forName(taskConsumerConfig.getConsumerClass())
                .getConstructor(ObjectNode.class)
                .newInstance(taskConsumerConfig.getConfig());

        TaskExecutionService taskExecutionService = new TaskExecutionService(taskConsumer, statusProducer, applicationConfig.getHandlerConfig());
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
                ServiceProvider.getTaskProviderService().init();
                break;
            case DEFAULT_PROFILE:
                ServiceProvider.getTaskReaderService().init();
                ServiceProvider.getTaskProviderService().init();
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
                ServiceProvider.getTaskProviderService().start();
                break;
            case DEFAULT_PROFILE:
                ServiceProvider.getTaskReaderService().start();
                ServiceProvider.getTaskProviderService().start();
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
                if (ServiceProvider.getTaskProviderService() != null) {
                    ServiceProvider.getTaskProviderService().stop();
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
                if (ServiceProvider.getTaskProviderService() != null) {
                    ServiceProvider.getTaskProviderService().stop();
                }
                if (ServiceProvider.getTaskExecutionService() != null) {
                    ServiceProvider.getTaskExecutionService().stop();
                }
                break;
        }
    }
}
