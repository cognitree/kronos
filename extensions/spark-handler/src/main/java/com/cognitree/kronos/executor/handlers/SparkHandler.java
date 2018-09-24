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

package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.executor.model.TaskResult;
import com.cognitree.kronos.model.Task;
import com.cognitree.spark.restclient.SparkRestClient;
import com.cognitree.spark.restclient.model.JobStatusResponse;
import com.cognitree.spark.restclient.model.JobStatusResponse.DriverState;
import com.cognitree.spark.restclient.model.JobSubmitRequest;
import com.cognitree.spark.restclient.model.JobSubmitResponse;
import com.cognitree.spark.restclient.model.KillJobResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.cognitree.spark.restclient.SparkRestClient.ClusterMode;
import static com.cognitree.spark.restclient.SparkRestClient.builder;

public class SparkHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(SparkHandler.class);
    private static final int STATUS_MONITORING_INTERVAL = 5000;
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    @Override
    public void init(ObjectNode handlerConfig) {

    }

    @Override
    public TaskResult handle(Task task) {
        logger.info("Received request to handle task {}", task);
        final Map<String, Object> taskProperties = task.getProperties();
        final String sparkVersion = (String) taskProperties.get("sparkVersion");
        final String masterHost = (String) taskProperties.get("masterHost");
        final Integer masterPort = (Integer) taskProperties.get("masterPort");
        final ClusterMode clusterMode = ClusterMode.valueOf((String) taskProperties.get("clusterMode"));
        final boolean secure = (boolean) taskProperties.getOrDefault("secure", false);
        final Integer monitoringInterval = (Integer) taskProperties.
                getOrDefault("monitoringInterval", STATUS_MONITORING_INTERVAL);

        final SparkRestClient sparkRestClient = builder()
                .masterHost(masterHost)
                .masterPort(masterPort)
                .sparkVersion(sparkVersion)
                .clusterMode(clusterMode)
                .isSecure(secure)
                .build();

        if (!taskProperties.containsKey("submitRequest")) {
            logger.error("Missing Spark job submit request, failing task {}", task);
            return new TaskResult(false, "missing Spark job submit request");
        }

        try {
            final JobSubmitRequest submitRequest =
                    MAPPER.convertValue(taskProperties.get("submitRequest"), JobSubmitRequest.class);
            final JobSubmitResponse jobSubmitResponse = sparkRestClient.submitJob(submitRequest);
            if (!jobSubmitResponse.getSuccess()) {
                logger.error("Unable to submit Spark job request. Response : {}", jobSubmitResponse);
                return new TaskResult(false, "Unable to submit Spark job request");
            }

            JobStatusResponse statusResponse = sparkRestClient.getJobStatus(jobSubmitResponse.getSubmissionId());
            long maxExecutionTime = task.getMaxExecutionTimeInMs();
            while (!statusResponse.getDriverState().isFinal() && maxExecutionTime > 0) {
                Thread.sleep(monitoringInterval);
                statusResponse = sparkRestClient.getJobStatus(jobSubmitResponse.getSubmissionId());
                maxExecutionTime -= monitoringInterval;
            }

            if (!statusResponse.getDriverState().isFinal() && maxExecutionTime < 0) {
                logger.error("Task {} exceeded max execution time allowed. Killing job with submission id {}",
                        task, jobSubmitResponse.getSubmissionId());
                final KillJobResponse killJobResponse = sparkRestClient.killJob(jobSubmitResponse.getSubmissionId());
                if (!killJobResponse.getSuccess()) {
                    logger.error("Unable to kill job with submission id {}, message {}",
                            jobSubmitResponse.getSubmissionId(), killJobResponse.getMessage());
                }
                return new TaskResult(false, "Spark job exceeded max execution time");
            }

            logger.info("Task {} finished execution with state {}", task, statusResponse.getDriverState());
            if (statusResponse.getDriverState() != DriverState.FINISHED) {
                return new TaskResult(false, "Spark job finished execution with failure state " + statusResponse.getDriverState());
            }
        } catch (Exception e) {
            logger.error("Error executing task {}", task, e);
            return new TaskResult(false, "Error executing Spark job task: " + e.getMessage());
        }
        return TaskResult.SUCCESS;
    }
}
