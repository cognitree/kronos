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

package com.cognitree.spark.restclient.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.cognitree.spark.restclient.model.Action.CreateSubmissionRequest;

public class JobSubmitRequest {

    private final Action action = CreateSubmissionRequest;
    private String mainClass;
    private String appResource;
    private List<String> appArgs = new ArrayList<>();
    private String clientSparkVersion;

    private Map<String, String> environmentVariables = new HashMap<>();
    private SparkProperties sparkProperties;

    public Action getAction() {
        return action;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getAppResource() {
        return appResource;
    }

    public void setAppResource(String appResource) {
        this.appResource = appResource;
    }

    public List<String> getAppArgs() {
        return appArgs;
    }

    public void setAppArgs(List<String> appArgs) {
        this.appArgs = appArgs;
    }

    public String getClientSparkVersion() {
        return clientSparkVersion;
    }

    public void setClientSparkVersion(String clientSparkVersion) {
        this.clientSparkVersion = clientSparkVersion;
    }

    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }

    public void setEnvironmentVariables(Map<String, String> environmentVariables) {
        this.environmentVariables = environmentVariables;
    }

    public SparkProperties getSparkProperties() {
        return sparkProperties;
    }

    public void setSparkProperties(SparkProperties sparkProperties) {
        this.sparkProperties = sparkProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobSubmitRequest)) return false;
        JobSubmitRequest that = (JobSubmitRequest) o;
        return action == that.action &&
                Objects.equals(mainClass, that.mainClass) &&
                Objects.equals(appResource, that.appResource) &&
                Objects.equals(appArgs, that.appArgs) &&
                Objects.equals(clientSparkVersion, that.clientSparkVersion) &&
                Objects.equals(environmentVariables, that.environmentVariables) &&
                Objects.equals(sparkProperties, that.sparkProperties);
    }

    @Override
    public int hashCode() {

        return Objects.hash(action, mainClass, appResource, appArgs, clientSparkVersion, environmentVariables, sparkProperties);
    }

    @Override
    public String toString() {
        return "JobSubmitRequest{" +
                "action=" + action +
                ", mainClass='" + mainClass + '\'' +
                ", appResource='" + appResource + '\'' +
                ", appArgs=" + appArgs +
                ", clientSparkVersion='" + clientSparkVersion + '\'' +
                ", environmentVariables=" + environmentVariables +
                ", sparkProperties=" + sparkProperties +
                '}';
    }

    public static class SparkProperties {
        @JsonProperty(value = "spark.app.name")
        private String appName;
        @JsonProperty(value = "spark.master")
        private String master;
        @JsonProperty(value = "spark.jars")
        private String jars;
        private Map<String, String> otherProperties = new HashMap<>();

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getMaster() {
            return master;
        }

        public void setMaster(String master) {
            this.master = master;
        }

        public String getJars() {
            return jars;
        }

        public void setJars(String jars) {
            this.jars = jars;
        }

        @JsonAnySetter
        void setOtherProperties(String key, String value) {
            this.otherProperties.put(key, value);
        }

        @JsonAnyGetter
        Map<String, String> getOtherProperties() {
            return this.otherProperties;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SparkProperties)) return false;
            SparkProperties that = (SparkProperties) o;
            return Objects.equals(appName, that.appName) &&
                    Objects.equals(master, that.master) &&
                    Objects.equals(jars, that.jars) &&
                    Objects.equals(otherProperties, that.otherProperties);
        }

        @Override
        public int hashCode() {

            return Objects.hash(appName, master, jars, otherProperties);
        }

        @Override
        public String toString() {
            return "SparkProperties{" +
                    "appName='" + appName + '\'' +
                    ", master='" + master + '\'' +
                    ", jars='" + jars + '\'' +
                    ", otherProperties=" + otherProperties +
                    '}';
        }
    }

}