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
import com.cognitree.kronos.executor.ExecutorConfig;
import com.cognitree.kronos.executor.TaskExecutionService;
import com.cognitree.kronos.queue.QueueConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.InputStream;

public class ApplicationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    @BeforeClass
    public static void init() throws Exception {
        initTaskSchedulerService();
        initTaskExecutionService();
    }

    private static void initTaskSchedulerService() throws Exception {
        InputStream schedulerConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("scheduler.yaml");
        SchedulerConfig schedulerConfig = MAPPER.readValue(schedulerConfigStream, SchedulerConfig.class);

        InputStream queueConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigStream, QueueConfig.class);

        TaskSchedulerService taskSchedulerService = new TaskSchedulerService(schedulerConfig, queueConfig);
        ServiceProvider.registerService(taskSchedulerService);
        taskSchedulerService.init();
        taskSchedulerService.start();
    }

    private static void initTaskExecutionService() throws Exception {
        InputStream executorConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("executor.yaml");
        ExecutorConfig executorConfig = MAPPER.readValue(executorConfigStream, ExecutorConfig.class);

        InputStream queueConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigStream, QueueConfig.class);

        TaskExecutionService taskExecutionService =
                new TaskExecutionService(executorConfig, queueConfig);
        ServiceProvider.registerService(taskExecutionService);
        taskExecutionService.init();
        taskExecutionService.start();
    }

    @AfterClass
    public static void stop() {
        TaskExecutionService.getService().stop();
        TaskSchedulerService.getService().stop();
    }

    @Before
    public void initialize() {
        // reinit will clear all the tasks from task provider
        TaskSchedulerService.getService().reinitTaskProvider();
    }
}
