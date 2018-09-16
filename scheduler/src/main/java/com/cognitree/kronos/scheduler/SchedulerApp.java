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

import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.scheduler.store.StoreProvider;
import com.cognitree.kronos.scheduler.store.StoreProviderConfig;
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
    private StoreProvider storeProvider;

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
        SchedulerConfig schedulerConfig = MAPPER.readValue(schedulerConfigAsStream, SchedulerConfig.class);
        final InputStream queueConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);

        final StoreProviderConfig storeProviderConfig = schedulerConfig.getStoreProviderConfig();
        storeProvider = (StoreProvider) Class.forName(storeProviderConfig.getProviderClass())
                .getConstructor().newInstance();
        storeProvider.init(storeProviderConfig.getConfig());

        NamespaceService namespaceService = new NamespaceService(storeProvider.getNamespaceStore());
        TaskService taskService = new TaskService(storeProvider.getTaskStore());
        WorkflowService workflowService = new WorkflowService(storeProvider.getWorkflowStore());
        JobService jobService = new JobService(storeProvider.getJobStore());
        WorkflowTriggerService workflowTriggerService =
                new WorkflowTriggerService(storeProvider.getWorkflowTriggerStore());
        MailService mailService = new MailService(schedulerConfig.getMailConfig());
        WorkflowSchedulerService workflowSchedulerService =
                new WorkflowSchedulerService(storeProvider.getQuartzJobStore());
        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(queueConfig);

        logger.info("Initializing scheduler app");
        // initialize all service
        namespaceService.init();
        taskService.init();
        workflowService.init();
        jobService.init();
        workflowTriggerService.init();
        mailService.init();
        workflowSchedulerService.init();
        taskSchedulerService.init();

        logger.info("Starting scheduler app");
        // start all service
        namespaceService.start();
        taskService.start();
        workflowService.start();
        jobService.start();
        workflowTriggerService.start();
        mailService.start();
        workflowSchedulerService.start();
        taskSchedulerService.start();
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
        if (storeProvider != null) {
            storeProvider.stop();
        }
    }
}

