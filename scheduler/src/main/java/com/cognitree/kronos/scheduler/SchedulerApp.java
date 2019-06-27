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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.queue.QueueService;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.StoreServiceConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static com.cognitree.kronos.queue.QueueService.SCHEDULER_QUEUE;

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

        final StoreServiceConfig storeServiceConfig = schedulerConfig.getStoreServiceConfig();
        final StoreService storeService = (StoreService) Class.forName(storeServiceConfig.getStoreServiceClass())
                .getConstructor(ObjectNode.class).newInstance(storeServiceConfig.getConfig());
        final NamespaceService namespaceService = new NamespaceService();
        final TaskService taskService = new TaskService();
        final WorkflowService workflowService = new WorkflowService();
        final JobService jobService = new JobService();
        final WorkflowTriggerService workflowTriggerService = new WorkflowTriggerService();
        final MailService mailService = new MailService(schedulerConfig.getMailConfig());
        final QueueService queueService = new QueueService(queueConfig, SCHEDULER_QUEUE);
        // The order between task scheduler and workflow scheduler service is of importance
        // task scheduler service should be started before workflow scheduler service.
        // Workflow scheduler services starts the quartz scheduler which in turn might schedule some tasks
        // based on misfire policies and if the task scheduler service is not initialized that, it will result in NPE.
        final TaskSchedulerService taskSchedulerService =
                new TaskSchedulerService(queueConfig.getPollIntervalInMs());
        final WorkflowSchedulerService workflowSchedulerService = new WorkflowSchedulerService();

        logger.info("Initializing scheduler app");
        // initialize all service
        storeService.init();
        namespaceService.init();
        taskService.init();
        workflowService.init();
        jobService.init();
        workflowTriggerService.init();
        mailService.init();
        queueService.init();
        taskSchedulerService.init();
        workflowSchedulerService.init();

        logger.info("Starting scheduler app");
        // start all service
        storeService.start();
        namespaceService.start();
        taskService.start();
        workflowService.start();
        jobService.start();
        workflowTriggerService.start();
        mailService.start();
        queueService.start();
        taskSchedulerService.start();
        workflowSchedulerService.start();

        startAddOnServices(schedulerConfig, queueConfig);
    }

    private void startAddOnServices(SchedulerConfig schedulerConfig, QueueConfig queueConfig) throws Exception {
        if (schedulerConfig.isEnableConfigurationService()) {
            final ConfigurationService configurationService = new ConfigurationService(queueConfig);
            configurationService.init();
            configurationService.start();
        }
    }

    public void stop() {
        logger.info("Stopping scheduler app");
        // stop services in the reverse order
        stopAddOnServices();
        if (WorkflowSchedulerService.getService() != null) {
            WorkflowSchedulerService.getService().stop();
        }
        if (TaskSchedulerService.getService() != null) {
            TaskSchedulerService.getService().stop();
        }
        if (QueueService.getService(SCHEDULER_QUEUE) != null) {
            QueueService.getService(SCHEDULER_QUEUE).stop();
        }
        if (MailService.getService() != null) {
            MailService.getService().stop();
        }
        if (WorkflowTriggerService.getService() != null) {
            WorkflowTriggerService.getService().stop();
        }
        if (JobService.getService() != null) {
            JobService.getService().stop();
        }
        if (WorkflowService.getService() != null) {
            WorkflowService.getService().stop();
        }
        if (TaskService.getService() != null) {
            TaskService.getService().stop();
        }
        if (NamespaceService.getService() != null) {
            NamespaceService.getService().stop();
        }
        Service storeProviderService = ServiceProvider.getService(StoreService.class.getSimpleName());
        if (storeProviderService != null) {
            storeProviderService.stop();
        }
    }

    private void stopAddOnServices() {
        if (ConfigurationService.getService() != null) {
            ConfigurationService.getService().stop();
        }
    }
}

