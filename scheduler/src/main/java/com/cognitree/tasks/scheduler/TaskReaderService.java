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

package com.cognitree.tasks.scheduler;

import com.cognitree.tasks.ApplicationConfig;
import com.cognitree.tasks.Service;
import com.cognitree.tasks.scheduler.readers.TaskDefinitionReader;
import com.cognitree.tasks.scheduler.readers.TaskDefinitionReaderConfig;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Task reader service is responsible for scheduling jobs for each {@link TaskDefinitionReader} configured
 * in {@link ApplicationConfig#getReaderConfig()}
 */
public final class TaskReaderService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskReaderService.class);

    private final Map<String, TaskDefinitionReader> taskReaders = new HashMap<>();
    private final Map<String, TaskDefinitionReaderConfig> readerConfigMap;
    private Scheduler scheduler;

    public TaskReaderService(Map<String, TaskDefinitionReaderConfig> readerConfigMap) {
        logger.info("Initializing task reader service with reader config {}", readerConfigMap);
        this.readerConfigMap = readerConfigMap;
    }

    @Override
    public void init() throws SchedulerException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        scheduler = StdSchedulerFactory.getDefaultScheduler();
        initReaders();
    }

    private void initReaders() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        for (Map.Entry<String, TaskDefinitionReaderConfig> readerConfigEntry : readerConfigMap.entrySet()) {
            final TaskDefinitionReaderConfig taskDefinitionReaderConfig = readerConfigEntry.getValue();
            final TaskDefinitionReader taskDefinitionReader = (TaskDefinitionReader) Class.forName(taskDefinitionReaderConfig.getReaderClass())
                    .newInstance();
            taskDefinitionReader.init(taskDefinitionReaderConfig.getConfig());
            taskReaders.put(readerConfigEntry.getKey(), taskDefinitionReader);
        }
    }

    @Override
    public void start() throws SchedulerException {
        scheduler.start();
        scheduleJobs();
    }

    /**
     * Schedule a job for each {@link TaskDefinitionReader} implementation to repeat
     * at every configured interval {@link TaskDefinitionReaderConfig#getSchedule()}
     */
    private void scheduleJobs() {
        readerConfigMap.forEach((readerName, taskDefinitionReaderConfig) -> {
            try {
                JobDataMap jobDataMap = new JobDataMap();
                jobDataMap.put("taskDefinitionReader", taskReaders.get(readerName));
                JobDetail jobDetail = newJob(TaskSchedulerJob.class)
                        .withIdentity(readerName, readerName + "jobScheduler")
                        .usingJobData(jobDataMap)
                        .build();
                Trigger trigger = newTrigger()
                        .withIdentity(readerName, readerName + "jobScheduler")
                        .withSchedule(CronScheduleBuilder.cronSchedule(taskDefinitionReaderConfig.getSchedule()))
                        .build();
                scheduler.scheduleJob(jobDetail, trigger);
            } catch (Exception e) {
                logger.error("Error scheduling job ", e);
            }
        });
    }

    @Override
    public void stop() {
        try {
            logger.info("stop: Stopping task reader service...");
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (Exception e) {
            logger.error("stop: Error stopping task reader service...", e);
        }
    }
}
