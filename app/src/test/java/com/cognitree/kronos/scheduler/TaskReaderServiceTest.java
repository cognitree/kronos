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
import com.cognitree.kronos.model.TaskDefinition;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.scheduler.SchedulerConfig;
import com.cognitree.kronos.scheduler.TaskProvider;
import com.cognitree.kronos.scheduler.TaskSchedulerService;
import com.cognitree.kronos.scheduler.TaskSchedulerServiceTest;
import com.cognitree.kronos.scheduler.readers.MockTaskDefinitionReader;
import com.cognitree.kronos.scheduler.readers.TaskReaderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.io.InputStream;

import static com.cognitree.kronos.TestUtil.sleep;

public class TaskReaderServiceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    @BeforeClass
    public static void init() throws Exception {
        InputStream schedulerConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("scheduler.yaml");
        SchedulerConfig schedulerConfig = MAPPER.readValue(schedulerConfigStream, SchedulerConfig.class);

        InputStream queueConfigStream =
                TaskSchedulerServiceTest.class.getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigStream, QueueConfig.class);
        TaskReaderService taskReaderService = new TaskReaderService(schedulerConfig.getTaskReaderConfig());
        ServiceProvider.registerService(taskReaderService);
        taskReaderService.init();
        taskReaderService.start();

        TaskSchedulerService taskSchedulerService =
                new TaskSchedulerService(schedulerConfig, queueConfig);
        ServiceProvider.registerService(taskSchedulerService);
        taskSchedulerService.init();
        taskSchedulerService.start();
    }

    @AfterClass
    public static void cleanup() {
        TaskReaderService.getService().stop();
        TaskSchedulerService.getService().stop();
    }

    @Test
    public void testScheduledTaskDefinitionReaderJob() throws SchedulerException {
        final Scheduler scheduler = TaskReaderService.getService().getScheduler();
        final JobKey mockReaderJobKey = new JobKey("mockReader", "mockReaderjobScheduler");
        Assert.assertTrue(scheduler.checkExists(mockReaderJobKey));

        final JobKey testReaderJobKey = new JobKey("testReader", "testReaderjobScheduler");
        Assert.assertFalse(scheduler.checkExists(testReaderJobKey));

    }

    @Test
    public void testScheduledTaskDefinitionJob() throws SchedulerException {
        final Scheduler scheduler = TaskReaderService.getService().getScheduler();
        sleep(1000);

        final JobKey taskOneDefinitionReaderJobKey =
                new JobKey("default:taskOne:test", "mockReaderGroup");
        Assert.assertTrue(scheduler.checkExists(taskOneDefinitionReaderJobKey));
        Assert.assertEquals(MockTaskDefinitionReader.getTaskDefinition("taskOne"),
                scheduler.getJobDetail(taskOneDefinitionReaderJobKey).getJobDataMap().get("taskDefinition"));

        final JobKey taskTwoDefinitionReaderJobKey =
                new JobKey("default:taskTwo:test", "mockReaderGroup");
        Assert.assertTrue(scheduler.checkExists(taskTwoDefinitionReaderJobKey));
        Assert.assertEquals(MockTaskDefinitionReader.getTaskDefinition("taskTwo"),
                scheduler.getJobDetail(taskTwoDefinitionReaderJobKey).getJobDataMap().get("taskDefinition"));

        MockTaskDefinitionReader.updateTaskDefinition("taskOne", "0/1 * * 1/1 * ? *");
        MockTaskDefinitionReader.removeTaskDefinition("taskTwo");
        sleep(1000);
        Assert.assertTrue(scheduler.checkExists(taskOneDefinitionReaderJobKey));
        Assert.assertFalse(scheduler.checkExists(taskTwoDefinitionReaderJobKey));
        Assert.assertEquals(MockTaskDefinitionReader.getTaskDefinition("taskOne"),
                scheduler.getJobDetail(taskOneDefinitionReaderJobKey).getJobDataMap().get("taskDefinition"));

        final TaskProvider taskProvider = TaskSchedulerService.getService().getTaskProvider();
        int taskCount = taskProvider.size();
        sleep(1000);
        Assert.assertEquals(++taskCount, taskProvider.size());
        sleep(1000);
        Assert.assertEquals(++taskCount, taskProvider.size());
        sleep(1000);
        Assert.assertEquals(++taskCount, taskProvider.size());
    }
}
