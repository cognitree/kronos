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
import com.cognitree.kronos.scheduler.readers.TaskDefinitionReader;
import com.cognitree.kronos.scheduler.readers.TaskDefinitionReaderConfig;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Task reader service schedules quartz job {@link TaskDefinitionReaderJob} for each {@link TaskDefinitionReader}
 * configured in {@link SchedulerConfig#getTaskReaderConfig()}
 */
public final class TaskReaderService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskReaderService.class);

    private final Map<String, TaskDefinitionReader> taskReaders = new HashMap<>();
    private final Map<String, TaskDefinitionReaderConfig> readerConfigMap;
    private Scheduler scheduler;

    public TaskReaderService(Map<String, TaskDefinitionReaderConfig> readerConfigMap) {
        this.readerConfigMap = readerConfigMap;
    }

    @Override
    public void init() throws Exception {
        scheduler = StdSchedulerFactory.getDefaultScheduler();
        initReaders();
    }

    private void initReaders() throws Exception {
        for (Map.Entry<String, TaskDefinitionReaderConfig> readerConfigEntry : readerConfigMap.entrySet()) {
            final String readerName = readerConfigEntry.getKey();
            final TaskDefinitionReaderConfig readerConfig = readerConfigEntry.getValue();
            logger.info("Initializing task definition reader with name {}, config {}", readerName, readerConfig);
            // validate reader schedule before instantiating
            CronExpression.validateExpression(readerConfig.getSchedule());
            final TaskDefinitionReader taskDefinitionReader = (TaskDefinitionReader) Class.forName(readerConfig.getReaderClass())
                    .newInstance();
            taskDefinitionReader.init(readerConfig.getConfig());
            taskReaders.put(readerName, taskDefinitionReader);
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
    private void scheduleJobs() throws SchedulerException {
        for (Map.Entry<String, TaskDefinitionReaderConfig> readerConfigEntry : readerConfigMap.entrySet()) {
            final String readerName = readerConfigEntry.getKey();
            final TaskDefinitionReaderConfig readerConfig = readerConfigEntry.getValue();
            logger.info("Starting task definition reader {} with config {}", readerName, readerConfig);
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("taskDefinitionReader", taskReaders.get(readerName));
            JobDetail jobDetail = newJob(TaskDefinitionReaderJob.class)
                    .withIdentity(readerName, readerName + "jobScheduler")
                    .usingJobData(jobDataMap)
                    .build();
            Trigger trigger = newTrigger()
                    .withIdentity(readerName, readerName + "jobScheduler")
                    .withSchedule(CronScheduleBuilder.cronSchedule(readerConfig.getSchedule()))
                    .build();
            scheduler.scheduleJob(jobDetail, trigger);
        }
    }

    @Override
    public void stop() {
        try {
            logger.info("Stopping task reader service...");
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (Exception e) {
            logger.error("Error stopping task reader service...", e);
        }
    }

    // used in junit
    public Scheduler getScheduler() {
        return scheduler;
    }
}
