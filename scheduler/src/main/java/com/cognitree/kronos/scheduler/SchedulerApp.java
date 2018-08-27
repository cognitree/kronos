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

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.queue.QueueConfig;
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
        logger.info("Starting scheduler app");
        final InputStream schedulerConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("scheduler.yaml");
        final SchedulerConfig schedulerConfig = MAPPER.readValue(schedulerConfigAsStream, SchedulerConfig.class);

        final InputStream queueConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("queue.yaml");
        final QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);

        // register service
        registerService(schedulerConfig, queueConfig);

        // initialize service
        NamespaceService.getService().init();
        TaskDefinitionService.getService().init();
        TaskService.getService().init();
        WorkflowDefinitionService.getService().init();
        WorkflowService.getService().init();
        WorkflowTriggerService.getService().init();
        TaskSchedulerService.getService().init();
        WorkflowSchedulerService.getService().init();

        // start service
        NamespaceService.getService().start();
        TaskDefinitionService.getService().start();
        TaskService.getService().start();
        WorkflowDefinitionService.getService().start();
        WorkflowService.getService().start();
        WorkflowTriggerService.getService().start();
        TaskSchedulerService.getService().start();
        WorkflowSchedulerService.getService().start();
    }

    private void registerService(SchedulerConfig schedulerConfig, QueueConfig queueConfig) {
        NamespaceService namespaceService =
                new NamespaceService(schedulerConfig.getNamespaceStoreConfig());
        ServiceProvider.registerService(namespaceService);

        TaskDefinitionService taskDefinitionService =
                new TaskDefinitionService(schedulerConfig.getTaskDefinitionStoreConfig());
        ServiceProvider.registerService(taskDefinitionService);

        TaskService taskService = new TaskService(schedulerConfig.getTaskStoreConfig());
        ServiceProvider.registerService(taskService);

        WorkflowDefinitionService workflowDefinitionService =
                new WorkflowDefinitionService(schedulerConfig.getWorkflowDefinitionStoreConfig());
        ServiceProvider.registerService(workflowDefinitionService);

        WorkflowService workflowService = new WorkflowService(schedulerConfig.getWorkflowStoreConfig());
        ServiceProvider.registerService(workflowService);

        WorkflowTriggerService workflowTriggerService =
                new WorkflowTriggerService(schedulerConfig.getWorkflowTriggerStoreConfig());
        ServiceProvider.registerService(workflowTriggerService);

        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(schedulerConfig, queueConfig);
        ServiceProvider.registerService(taskSchedulerService);

        WorkflowSchedulerService workflowSchedulerService = new WorkflowSchedulerService();
        ServiceProvider.registerService(workflowSchedulerService);
    }

    public void stop() {
        logger.info("Stopping scheduler app");
        // stop services in the reverse order
        if (WorkflowSchedulerService.getService() != null) {
            WorkflowSchedulerService.getService().stop();
        }
        if (TaskSchedulerService.getService() != null) {
            TaskSchedulerService.getService().stop();
        }
        if (WorkflowTriggerService.getService() != null) {
            WorkflowTriggerService.getService().stop();
        }
        if (WorkflowService.getService() != null) {
            WorkflowService.getService().stop();
        }
        if (WorkflowDefinitionService.getService() != null) {
            WorkflowDefinitionService.getService().stop();
        }
        if (TaskService.getService() != null) {
            TaskService.getService().stop();
        }
        if (TaskDefinitionService.getService() != null) {
            TaskDefinitionService.getService().stop();
        }
        if (NamespaceService.getService() != null) {
            NamespaceService.getService().stop();
        }
    }
}

