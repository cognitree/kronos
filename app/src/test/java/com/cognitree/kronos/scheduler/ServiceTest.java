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

import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.executor.ExecutorConfig;
import com.cognitree.kronos.executor.TaskExecutionService;
import com.cognitree.kronos.queue.QueueService;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.InputStream;
import java.util.List;

public class ServiceTest {
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();
    private static final ExecutorApp EXECUTOR_APP = new ExecutorApp();
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static List<Namespace> existingNamespaces;

    @BeforeClass
    public static void start() throws Exception {
        SCHEDULER_APP.start();
        EXECUTOR_APP.start();
        existingNamespaces = NamespaceService.getService().get();
        createTopics();
    }

    private static void createTopics() throws ServiceException, java.io.IOException {
        // initial call so that the topics are created
        QueueService.getService(QueueService.SCHEDULER_QUEUE).consumeTaskStatusUpdates();
        QueueService.getService(QueueService.SCHEDULER_QUEUE).consumeControlMessages();
        final InputStream executorConfigAsStream =
                ServiceTest.class.getClassLoader().getResourceAsStream("executor.yaml");
        ExecutorConfig executorConfig = MAPPER.readValue(executorConfigAsStream, ExecutorConfig.class);
        executorConfig.getTaskHandlerConfig().forEach((type, taskHandlerConfig) -> {
            try {
                QueueService.getService(QueueService.EXECUTOR_QUEUE)
                        .consumeTask(type, 0);
            } catch (ServiceException e) {
                // do nothing
            }
        });
    }

    @AfterClass
    public static void stop() throws Exception {
        List<Namespace> namespaces = NamespaceService.getService().get();
        namespaces.removeAll(existingNamespaces);
        cleanupStore(namespaces);
        SCHEDULER_APP.stop();
        EXECUTOR_APP.stop();
        // cleanup queue
        QueueService.getService(QueueService.SCHEDULER_QUEUE).destroy();
        QueueService.getService(QueueService.EXECUTOR_QUEUE).destroy();
    }

    private static void cleanupStore(List<Namespace> namespaces) throws Exception {
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        for (Namespace namespace : namespaces) {
            List<Workflow> workflows = WorkflowService.getService().get(namespace.getName());
            for (Workflow workflow : workflows) {
                WorkflowService.getService().delete(workflow);
            }
            NamespaceStore namespaceStore = storeService.getNamespaceStore();
            namespaceStore.delete(Namespace.build(namespace.getName()));
        }
    }
}
