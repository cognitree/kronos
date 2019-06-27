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

package com.cognitree.kronos.executor;

import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.queue.QueueService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static com.cognitree.kronos.queue.QueueService.EXECUTOR_QUEUE;

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
            logger.error("Error starting executor app", e);
            System.exit(0);
        }
    }

    public void start() throws Exception {
        final InputStream executorConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("executor.yaml");
        ExecutorConfig executorConfig = MAPPER.readValue(executorConfigAsStream, ExecutorConfig.class);
        final InputStream queueConfigAsStream =
                getClass().getClassLoader().getResourceAsStream("queue.yaml");
        final QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);
        final QueueService queueService = new QueueService(queueConfig, EXECUTOR_QUEUE);
        final TaskExecutionService taskExecutionService =
                new TaskExecutionService(executorConfig.getTaskHandlerConfig(), queueConfig.getPollIntervalInMs());
        logger.info("Initializing executor app");
        queueService.init();
        taskExecutionService.init();
        logger.info("Starting executor app");
        queueService.start();
        taskExecutionService.start();
    }

    public void stop() {
        logger.info("Stopping executor app");
        if (TaskExecutionService.getService() != null) {
            TaskExecutionService.getService().stop();
        }
        if (QueueService.getService(EXECUTOR_QUEUE) != null) {
            QueueService.getService(EXECUTOR_QUEUE).stop();
        }
    }
}


