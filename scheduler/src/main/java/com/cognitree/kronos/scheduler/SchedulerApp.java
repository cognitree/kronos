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

import com.cognitree.kronos.ReviewPending;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.scheduler.store.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * starts the scheduler app by reading configurations from classpath.
 */
@ReviewPending
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

        // register service
        registerService(schedulerConfig, queueConfig);

        // initialize service
        TaskDefinitionStoreService.getService().init();
        TaskStoreService.getService().init();
        WorkflowDefinitionStoreService.getService().init();
        WorkflowStoreService.getService().init();
        TaskSchedulerService.getService().init();
        WorkflowSchedulerService.getService().init();

        // start service
        TaskDefinitionStoreService.getService().start();
        TaskStoreService.getService().start();
        WorkflowDefinitionStoreService.getService().start();
        WorkflowStoreService.getService().start();
        TaskSchedulerService.getService().start();
        WorkflowSchedulerService.getService().start();
    }

    private void registerService(SchedulerConfig schedulerConfig, QueueConfig queueConfig) {
        TaskDefinitionStoreService taskDefinitionStoreService =
                new TaskDefinitionStoreService(schedulerConfig.getTaskDefinitionStoreConfig());
        StoreServiceProvider.registerStoreService(taskDefinitionStoreService);

        TaskStoreService taskStoreService = new TaskStoreService(schedulerConfig.getTaskStoreConfig());
        StoreServiceProvider.registerStoreService(taskStoreService);

        WorkflowDefinitionStoreService workflowDefinitionStoreService =
                new WorkflowDefinitionStoreService(schedulerConfig.getWorkflowDefinitionStoreConfig());
        StoreServiceProvider.registerStoreService(workflowDefinitionStoreService);

        WorkflowStoreService workflowStoreService = new WorkflowStoreService(schedulerConfig.getWorkflowStoreConfig());
        StoreServiceProvider.registerStoreService(workflowStoreService);

        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(schedulerConfig, queueConfig);
        ServiceProvider.registerService(taskSchedulerService);

        WorkflowSchedulerService workflowSchedulerService = new WorkflowSchedulerService();
        ServiceProvider.registerService(workflowSchedulerService);
    }

    public void stop() {
        if (WorkflowSchedulerService.getService() != null) {
            WorkflowSchedulerService.getService().stop();
        }
        if (TaskSchedulerService.getService() != null) {
            TaskSchedulerService.getService().stop();
        }
        // stop store services
        if (TaskDefinitionStoreService.getService() != null) {
            TaskDefinitionStoreService.getService().stop();
        }
        if (TaskStoreService.getService() != null) {
            TaskStoreService.getService().stop();
        }
        if (WorkflowDefinitionStoreService.getService() != null) {
            WorkflowDefinitionStoreService.getService().stop();
        }
        if (WorkflowStoreService.getService() != null) {
            WorkflowStoreService.getService().stop();
        }
    }
}

