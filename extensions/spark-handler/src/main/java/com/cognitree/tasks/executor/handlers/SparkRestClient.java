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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.fasterxml.jackson.databind.DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS;

/**
 * Prepares an http request for the given spark job config and invokes the
 * Rest api on the spark master
 */
public class SparkRestClient {

    private static final Logger logger = LoggerFactory.getLogger(SparkRestClient.class);

    private static final Set<String> finishedStates = new HashSet<>();
    private static final int DEFAULT_MONITORING_INTERVAL = 60000;
    private static final SparkRestClient instance = new SparkRestClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().
            configure(UNWRAP_SINGLE_VALUE_ARRAYS, true).
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        finishedStates.add("FINISHED");
        finishedStates.add("KILLED");
        finishedStates.add("FAILED");
        finishedStates.add("ERROR");
        //All possible states below
        //SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR
    }

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private SparkRestClient() {
    }

    public static SparkRestClient getInstance() {
        return instance;
    }

    public String submitSparkApp(SparkJobConfig jobConfig, SparkCluster sparkConfig,
                                 Task task, SparkJobStateListener jobStateListener) throws IOException, InterruptedException {
        sparkConfig.setClientSparkVersion(sparkConfig.getClientSparkVersion());
        jobConfig.setAction("CreateSubmissionRequest");
        jobConfig.setClientSparkVersion(sparkConfig.getClientSparkVersion());
        jobConfig.getSparkProperties().setProperty("spark.master", sparkConfig.getMaster());
        logger.info("submitSparkApp: jobConfig={} and sparkConfig={}", jobConfig, sparkConfig);

        HttpEntity entity = EntityBuilder.create()
                .setContentType(ContentType.APPLICATION_JSON)
                .setText(OBJECT_MAPPER.writeValueAsString(jobConfig))
                .build();
        HttpUriRequest sparkSubmitRestRequest = RequestBuilder
                .post()
                .setUri(sparkConfig.getRestBaseURL() + "/v1/submissions/create")
                .setEntity(entity)
                .build();
        CloseableHttpResponse response = HttpClientBuilder.create().build().execute(sparkSubmitRestRequest);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map submitResponse = OBJECT_MAPPER.readValue(responseBody, Map.class);
        StatusLine statusLine = response.getStatusLine();
        logResponse(jobConfig, responseBody, statusLine);

        String submissionId = submitResponse.get("submissionId").toString();
        logger.info("submitted successfully: submission id is: {}", submissionId);
        if (submissionId != null) {
            int interval = jobConfig.getMonitoringInterval() > 0 ? jobConfig.getMonitoringInterval() : DEFAULT_MONITORING_INTERVAL;
            executorService.schedule(() -> getSubmissionStatus(submissionId, sparkConfig, task, jobStateListener, interval), interval, TimeUnit.MILLISECONDS);
        }

        return submissionId;
    }

    private void getSubmissionStatus(String submissionId, SparkCluster config, Task task, SparkJobStateListener jobStateListener, int monitoringInterval) {
        HttpUriRequest statusRequest = RequestBuilder.get()
                .setUri(config.getRestBaseURL() + "/v1/submissions/status/" + submissionId)
                .build();
        try {
            CloseableHttpResponse response = HttpClientBuilder.create().build().execute(statusRequest);
            String responseBody = EntityUtils.toString(response.getEntity());
            Properties submitResponse = OBJECT_MAPPER.readValue(responseBody, Properties.class);
            logResponse(statusRequest, responseBody, response.getStatusLine());
            String driverState = submitResponse.getProperty("driverState");
            logger.info("Status of the submitted spark application with submission id:{} is {}", submissionId, driverState);
            if (driverState != null && finishedStates.contains(driverState)) {
                if ("FINISHED".equalsIgnoreCase(driverState)) {
                    jobStateListener.stateChangeNotification(task, SparkAppHandle.State.FINISHED);
                } else {
                    jobStateListener.stateChangeNotification(task, SparkAppHandle.State.FAILED);
                }
            } else {
                executorService.schedule(() -> getSubmissionStatus(submissionId, config, task, jobStateListener, monitoringInterval),
                        monitoringInterval, TimeUnit.MILLISECONDS);
            }
        } catch (IOException e) {
            logger.error("Caught exception. Error Message: {}", e.getMessage());
        }

    }

    private void logResponse(Object request, String responseBody, StatusLine statusLine) {
        if (statusLine.getStatusCode() < 400) {
            logger.info("SparkRestClient: Request={} & Response={} & ReturnCode={}",
                    request, responseBody, statusLine);
        } else {
            logger.error("SparkRestClient: Request={} & Response={} & ReturnCode={}",
                    request, responseBody, statusLine);
        }
    }
}
