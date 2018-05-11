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

package com.cognitree.tasks.executor.handlers;

import com.cognitree.tasks.Service;
import com.cognitree.tasks.executor.handlers.model.SparkCluster;
import com.cognitree.tasks.executor.handlers.model.SparkClusterConfig;
import com.cognitree.tasks.executor.handlers.model.SparkJobConfig;
import com.cognitree.tasks.model.Task;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class SparkTaskRunnerService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(SparkTaskRunnerService.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String SPARKENV_PREFIX = "sparkenv";
    private static final int SPARK_ENV_LENGTH = "sparkenv".length() + 1;
    private static final String SPARKJOB_PREFIX = "sparkjob";
    private static final int SPARK_JOB_LENGTH = "sparkjob".length() + 1;
    private final SparkClusterConfig sparkClusterConfig;

    public SparkTaskRunnerService(SparkClusterConfig sparkClusterConfig) {
        this.sparkClusterConfig = sparkClusterConfig;
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() {

    }

    public void execute(Task task, ObjectNode taskConfig, SparkJobStateListener jobStateListener) {
        logger.info("execute: Executing {}", task);

        try {
            Map<String, SparkCluster> sparkClusters = sparkClusterConfig.getSparkClusters();

            SparkJobConfig sparkJobConfig =
                    (SparkJobConfig) OBJECT_MAPPER.convertValue(taskConfig, SparkJobConfig.class).clone();

            final Map<String, Object> taskProperties = task.getProperties();
            overrideSparkJobConfig(taskProperties, sparkJobConfig);

            SparkCluster sparkCluster =
                    sparkClusters.getOrDefault(sparkJobConfig.getClusterName(), sparkClusters.get("default"));
            overrideSparkEnv(taskProperties, sparkCluster);
            if (task.getName() != null && !task.getName().isEmpty()) {
                String appName = task.getGroup() != null ? task.getGroup() + "-" + task.getName() : task.getName();
                sparkJobConfig.getSparkProperties().setProperty("spark.app.name", appName);
            }
            String appId;

            if ("pyspark".equals(sparkJobConfig.getAppType())) {
                appId = SparkSubmitClient.getInstance().submitSparkApp(sparkJobConfig, sparkCluster, task, jobStateListener);
            } else {
                appId = SparkRestClient.getInstance().submitSparkApp(sparkJobConfig, sparkCluster, task, jobStateListener);
            }

            if (appId == null) {
                jobStateListener.stateChangeNotification(task, SparkAppHandle.State.FAILED);
            } else {
                logger.info("Spark task with id: {}, submitted successfully", task.getId());
            }
        } catch (Exception ex) {
            jobStateListener.stateChangeNotification(task, SparkAppHandle.State.FAILED);
            logger.error("Error while submitting spark application: {}. Reason: {}", this, ex.getMessage(), ex);
        }
    }

    private void overrideSparkJobConfig(Map<String, Object> taskProperties, SparkJobConfig sparkJobConfig) {
        String[] appArgs = sparkJobConfig.getAppArgs();
        Properties context = new Properties();
        context.putAll(taskProperties);
        ArrayList<String> contextArgs = new ArrayList<>();
        if (appArgs != null && appArgs.length > 0) {
            contextArgs.addAll(Arrays.asList(appArgs));
        }
        context.stringPropertyNames().forEach(key -> {
            String newKey = escapeKey(key);
            if (key.startsWith(SPARKJOB_PREFIX)) {
                sparkJobConfig.set(newKey.substring(SPARK_JOB_LENGTH), context.get(key).toString());
            } else if (key.startsWith("--")) {
                contextArgs.add(newKey + "=" + context.getProperty(key));
            }
        });
        sparkJobConfig.setAppArgs(contextArgs.toArray(new String[]{}));
    }

    private String escapeKey(String key) {
        return key.replaceAll("\\\u2024", "\\.");
    }

    private void overrideSparkEnv(Map<String, Object> context, SparkCluster sparkCluster) {

        context.keySet().forEach(key -> {
            String newKey = escapeKey(key);
            if (key.startsWith(SPARKENV_PREFIX)) {
                sparkCluster.set(newKey.substring(SPARK_ENV_LENGTH), context.get(key).toString());
            }
        });
    }
}
