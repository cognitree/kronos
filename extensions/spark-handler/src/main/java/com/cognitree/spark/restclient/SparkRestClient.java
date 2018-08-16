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

package com.cognitree.spark.restclient;

import com.cognitree.spark.restclient.model.JobStatusResponse;
import com.cognitree.spark.restclient.model.JobSubmitRequest;
import com.cognitree.spark.restclient.model.JobSubmitResponse;
import com.cognitree.spark.restclient.model.KillJobResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.apache.http.protocol.HTTP.CONTENT_TYPE;

public class SparkRestClient {
    private static final Logger logger = LoggerFactory.getLogger(SparkRestClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SUBMIT_JOB_URL = "/v1/submissions/create";
    private static final String KILL_JOB_URL = "/v1/submissions/kill";
    private static final String JOB_STATUS_URL = "/v1/submissions/status";

    private String masterUrl;
    private ClusterMode clusterMode;
    private String sparkVersion;
    private HttpClient client;
    private boolean secure;

    private SparkRestClient() {
    }

    public static SparkRestClientBuilder builder() {
        return new SparkRestClientBuilder();
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public ClusterMode getClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(ClusterMode clusterMode) {
        this.clusterMode = clusterMode;
    }

    public String getSparkVersion() {
        return sparkVersion;
    }

    public void setSparkVersion(String sparkVersion) {
        this.sparkVersion = sparkVersion;
    }

    public HttpClient getClient() {
        return client;
    }

    public void setClient(HttpClient client) {
        this.client = client;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public JobSubmitResponse submitJob(JobSubmitRequest jobSubmitRequest) throws Exception {
        logger.debug("Received request to submit Spark job with request {}", jobSubmitRequest);
        // override spark version if specified at rest client
        jobSubmitRequest.setClientSparkVersion(sparkVersion);
        // override spark master property in submit request
        validateJobSubmitRequest(jobSubmitRequest);
        addAppResourceToJars(jobSubmitRequest);
        jobSubmitRequest.getSparkProperties().setMaster(clusterMode.name() + "://" + masterUrl);
        final String url = getMasterRestUrl() + SUBMIT_JOB_URL;
        final HttpPost post = new HttpPost(url);
        post.setHeader(CONTENT_TYPE, APPLICATION_JSON.toString());
        post.setEntity(new StringEntity(MAPPER.writeValueAsString(jobSubmitRequest)));
        return execute(post, JobSubmitResponse.class);
    }

    private void validateJobSubmitRequest(JobSubmitRequest jobSubmitRequest) {
        final String appName = jobSubmitRequest.getSparkProperties().getAppName();
        if (appName == null || appName.isEmpty()) {
            logger.error("app name for Spark job is not set");
            throw new IllegalArgumentException("app name for Spark job is not set");
        }

        final String appResource = jobSubmitRequest.getAppResource();
        if (appResource == null || appResource.isEmpty()) {
            logger.error("app resource for Spark job is not set");
            throw new IllegalArgumentException("app resource for Spark job is not set");
        }

        final String mainClass = jobSubmitRequest.getMainClass();
        if (mainClass == null || mainClass.isEmpty()) {
            logger.error("main class for Spark job is not set");
            throw new IllegalArgumentException("main class for Spark job is not set");
        }
    }

    private void addAppResourceToJars(JobSubmitRequest jobSubmitRequest) {
        String jars = jobSubmitRequest.getSparkProperties().getJars();
        jars = jars == null || jars.trim().isEmpty() ? jobSubmitRequest.getAppResource() :
                jobSubmitRequest.getAppResource() + "," + jars;
        jobSubmitRequest.getSparkProperties().setJars(jars);
    }

    public JobStatusResponse getJobStatus(String submissionId) throws Exception {
        logger.debug("Received request to query Spark job status with submission id {}", submissionId);
        final String url = getMasterRestUrl() + JOB_STATUS_URL + "/" + submissionId;
        return execute(new HttpGet(url), JobStatusResponse.class);
    }

    public KillJobResponse killJob(String submissionId) throws Exception {
        logger.debug("Received request to kill Spark job with submission id {}", submissionId);
        final String url = getMasterRestUrl() + KILL_JOB_URL + "/" + submissionId;
        return execute(new HttpPost(url), KillJobResponse.class);
    }

    private String getMasterRestUrl() {
        return isSecure() ? "https" : "http" + "://" + masterUrl;
    }

    private <T> T execute(HttpRequestBase httpRequest, Class<T> responseClass) throws Exception {
        try {
            final String stringResponse = client.execute(httpRequest, new BasicResponseHandler());
            return MAPPER.readValue(stringResponse, responseClass);
        } finally {
            httpRequest.releaseConnection();
        }
    }

    public enum ClusterMode {
        spark, yarn
    }

    public static class SparkRestClientBuilder {
        private String sparkVersion;
        private Integer masterPort = 6066;
        private String masterHost = "localhost";
        private boolean secure = false;
        private ClusterMode clusterMode = ClusterMode.spark;

        private HttpClient client = HttpClientBuilder.create()
                .setConnectionManager(new BasicHttpClientConnectionManager())
                .build();

        private SparkRestClientBuilder() {
        }

        public SparkRestClientBuilder sparkVersion(String sparkVersion) {
            this.sparkVersion = sparkVersion;
            return this;
        }

        public SparkRestClientBuilder masterPort(Integer masterPort) {
            this.masterPort = masterPort;
            return this;
        }

        public SparkRestClientBuilder isSecure(boolean isSecure) {
            this.secure = isSecure;
            return this;
        }

        public SparkRestClientBuilder masterHost(String masterHost) {
            this.masterHost = masterHost;
            return this;
        }

        public SparkRestClientBuilder clusterMode(ClusterMode clusterMode) {
            this.clusterMode = clusterMode;
            return this;
        }

        public SparkRestClientBuilder httpClient(HttpClient httpClient) {
            this.client = httpClient;
            return this;
        }

        public SparkRestClientBuilder poolingHttpClient(int maxTotalConnections) {
            final PoolingHttpClientConnectionManager poolingHttpClientConnectionManager
                    = new PoolingHttpClientConnectionManager();
            poolingHttpClientConnectionManager.setMaxTotal(maxTotalConnections);
            poolingHttpClientConnectionManager.setDefaultMaxPerRoute(maxTotalConnections);
            this.client = HttpClientBuilder.create()
                    .setConnectionManager(poolingHttpClientConnectionManager)
                    .build();
            return this;
        }

        public SparkRestClient build() {
            if (masterHost == null) {
                logger.error("Spark master hostname is not set");
                throw new IllegalArgumentException("master host must be set.");
            }
            if (client == null) {
                logger.error("HTTP client is not set");
                throw new IllegalArgumentException("HTTP client cannot be null.");
            }
            if (sparkVersion == null || sparkVersion.isEmpty()) {
                logger.error("Spark client version is not set");
                throw new IllegalArgumentException("Spark client version is not set.");
            }

            final String masterUrl;
            if (masterPort != null) {
                masterUrl = masterHost + ":" + masterPort;
            } else {
                masterUrl = masterHost;
            }

            logger.info("Building Spark REST client with master URL: {}, client version: {}, cluster mode: {}," +
                    " HTTPS enabled: {}", masterUrl, sparkVersion, clusterMode, secure);

            SparkRestClient sparkRestClient = new SparkRestClient();
            sparkRestClient.setMasterUrl(masterUrl);
            sparkRestClient.setSparkVersion(sparkVersion);
            sparkRestClient.setClient(client);
            sparkRestClient.setSecure(secure);
            sparkRestClient.setClusterMode(clusterMode);
            return sparkRestClient;
        }
    }
}