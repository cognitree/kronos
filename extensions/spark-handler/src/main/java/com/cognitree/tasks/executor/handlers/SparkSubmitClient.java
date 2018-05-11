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

import com.cognitree.tasks.executor.handlers.model.SparkCluster;
import com.cognitree.tasks.executor.handlers.model.SparkJobConfig;
import com.cognitree.tasks.model.Task;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.time.format.DateTimeFormatter.ofPattern;

/**
 * Launches a spark job for the given spark job configuration inline in client mode.
 * This client is meant to be used for pyspark applications which can only run in client mode
 * on spark-standalone-cluster. For spark jobs written in scala/java SparkRestClient can be used to
 * submit to the spark cluster.
 */
public class SparkSubmitClient {

    private static final Logger logger = LoggerFactory.getLogger(SparkSubmitClient.class.getSimpleName());

    private static final String DEPLOY_MODE_CLIENT = "client";

    private static final SparkSubmitClient instance = new SparkSubmitClient();

    private SparkSubmitClient() {
    }

    public static SparkSubmitClient getInstance() {
        return instance;
    }

    public String submitSparkApp(SparkJobConfig jobConfig, SparkCluster sparkConfig, Task task,
                                 SparkJobStateListener jobStateListener) throws IOException {
        logger.info("submitSparkApp: jobConfig={} and sparkConfig={}", jobConfig, sparkConfig);
        String appName = jobConfig.getSparkProperties().getProperty("spark.app.name");
        String dateTime = Instant.now().atOffset(ZoneOffset.UTC).format(ofPattern("yyyyMMdd_HHmmss"));
        SparkLauncher sparkLauncher = new SparkLauncher(toMap(jobConfig.getEnvironmentVariables()))
                .setMaster(sparkConfig.getMaster())
                .setAppName(appName)
                .setAppResource(jobConfig.getAppResource())
                .setSparkHome(sparkConfig.getSparkHomeDir())
                .directory(new File(sparkConfig.getSparkHomeDir()))
                .setDeployMode(DEPLOY_MODE_CLIENT)
                .redirectError(new File(jobConfig.getLogDir(), appName + "_" + dateTime + "_stderr.log"))
                .redirectOutput(new File(jobConfig.getLogDir(), appName + "_" + dateTime + "_stdout.log"))
                .addAppArgs(jobConfig.getAppArgs());

        jobConfig.getSparkProperties().stringPropertyNames().forEach(key -> {
            if (key.startsWith("spark.")) {
                sparkLauncher.setConf(key, jobConfig.getSparkProperties().getProperty(key));
            } else if (key.startsWith("--")) {
                sparkLauncher.addSparkArg(key, jobConfig.getSparkProperties().getProperty(key));
            }
        });
        SparkAppHandle sparkAppHandle = sparkLauncher.startApplication();
        sparkAppHandle.addListener(new SparkAppHandleListener(jobConfig, task, jobStateListener));
        logger.info("submitSparkApp: Submitted {}", appName);
        while (sparkAppHandle.getAppId() == null) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return sparkAppHandle.getAppId();
    }

    private Map<String, String> toMap(Properties props) {
        Map<String, String> res = new HashMap<>();
        props.stringPropertyNames().forEach(key -> res.put(key, props.getProperty(key)));
        return res;
    }

    public static class SparkAppHandleListener implements SparkAppHandle.Listener {

        private final SparkJobConfig jobConfig;
        private final SparkJobStateListener jobStateListener;
        private final Task task;

        public SparkAppHandleListener(SparkJobConfig jobConfig, Task task, SparkJobStateListener jobStateListener) {
            this.jobConfig = jobConfig;
            this.jobStateListener = jobStateListener;
            this.task = task;
        }

        @Override
        public void stateChanged(SparkAppHandle sparkAppHandle) {
            SparkAppHandle.State state = sparkAppHandle.getState();
            jobStateListener.stateChangeNotification(task, state);
            if (sparkAppHandle.getAppId() != null) {
                logger.info("[{}:{}] JobConfig={}", sparkAppHandle.getState(), sparkAppHandle.getAppId(), jobConfig);
            } else {
                logger.info("[{}] JobConfig={}", sparkAppHandle.getState(), jobConfig);
            }

        }

        @Override
        public void infoChanged(SparkAppHandle sparkAppHandle) {
            if (sparkAppHandle.getAppId() != null) {
                logger.info("[{}:{}] JobConfig={}", sparkAppHandle.getState(), sparkAppHandle.getAppId(), jobConfig);
            } else {
                logger.info("[{}] JobConfig={}", sparkAppHandle.getState(), jobConfig);
            }
        }
    }
}
