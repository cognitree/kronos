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
import java.util.ArrayList;
import java.util.List;

public class ServiceTest {
    static final String WORKFLOW_TEMPLATE_YAML = "workflows/workflow-template.yaml";
    static final String WORKFLOW_TEMPLATE_TIMEOUT_TASKS_YAML = "workflows/workflow-template-timeout-tasks.yaml";
    static final String WORKFLOW_TEMPLATE_TIMEOUT_TASKS_WITH_RETRY_YAML = "workflows/workflow-template-timeout-tasks-with-retry.yaml";
    static final String WORKFLOW_TEMPLATE_FAILED_HANDLER_YAML = "workflows/workflow-template-failed-handler.yaml";
    static final String INVALID_WORKFLOW_MISSING_TASKS_TEMPLATE_YAML = "workflows/invalid-workflow-missing-tasks-template.yaml";
    static final String INVALID_WORKFLOW_DISABLED_TASKS_TEMPLATE_YAML = "workflows/invalid-workflow-disabled-tasks-template.yaml";
    static final String WORKFLOW_TEMPLATE_ABORT_TASKS_YAML = "workflows/workflow-template-abort-tasks.yaml";
    static final String WORKFLOW_TEMPLATE_WITH_TASK_CONTEXT_YAML = "workflows/workflow-template-with-task-context.yaml";
    static final String WORKFLOW_TEMPLATE_WITH_PROPERTIES_YAML = "workflows/workflow-template-with-properties.yaml";
    static final String WORKFLOW_TEMPLATE_WITH_DUPLICATE_POLICY_YAML = "workflows/workflow-template-with-duplicate-policy.yaml";

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();
    private static final ExecutorApp EXECUTOR_APP = new ExecutorApp();
    private static final List<Namespace> EXISTING_NAMESPACE = new ArrayList<>();

    @BeforeClass
    public static void start() throws Exception {
        SCHEDULER_APP.start();
        EXECUTOR_APP.start();
        EXISTING_NAMESPACE.addAll(NamespaceService.getService().get());
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
        namespaces.removeAll(EXISTING_NAMESPACE);
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
