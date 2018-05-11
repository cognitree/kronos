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

package com.cognitree.tasks.executor.handlers.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

public class SparkJobConfig implements Serializable, Cloneable {

    private static final String PREFIX_ENV = "env";
    private String clientSparkVersion;
    private String action;
    private String appResource;
    private String mainClass;
    private String[] appArgs;
    private Properties sparkProperties;
    private Properties environmentVariables;
    private String appType;
    private int monitoringInterval;
    private String clusterName;
    private String logDir;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String[] getAppArgs() {
        return appArgs;
    }

    public void setAppArgs(String[] appArgs) {
        this.appArgs = appArgs;
    }

    public String getAppResource() {
        return appResource;
    }

    public void setAppResource(String appResource) {
        this.appResource = appResource;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public Properties getEnvironmentVariables() {
        return environmentVariables;
    }

    public void setEnvironmentVariables(Properties environmentVariables) {
        this.environmentVariables = environmentVariables;
    }

    public Properties getSparkProperties() {
        return sparkProperties;
    }

    public void setSparkProperties(Properties sparkProperties) {
        this.sparkProperties = sparkProperties;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getClientSparkVersion() {
        return clientSparkVersion;
    }

    public void setClientSparkVersion(String clientSparkVersion) {
        this.clientSparkVersion = clientSparkVersion;
    }

    public String getAppType() {
        return appType;
    }

    public void setAppType(String appType) {
        this.appType = appType;
    }

    public String getLogDir() {
        return logDir != null ? logDir : System.getProperty("java.io.tmpdir");
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public int getMonitoringInterval() {
        return monitoringInterval;
    }

    public void setMonitoringInterval(int monitoringInterval) {
        this.monitoringInterval = monitoringInterval;
    }

    public void set(String key, String value) {
        switch (key) {
            case "clientSparkVersion":
                setClientSparkVersion(value);
                break;
            case "action":
                setAction(value);
                break;
            case "appResource":
                setAppResource(value);
                break;
            case "mainClass":
                setMainClass(value);
                break;
            case "appType":
                setAppType(value);
                break;
            case "monitoringInterval":
                setMonitoringInterval(Integer.valueOf(value));
                break;
            case "clusterName":
                setClusterName(value);
                break;
            case "logDir":
                setLogDir(value);
                break;
            default:
                if (key.startsWith(PREFIX_ENV)) {
                    getEnvironmentVariables().setProperty(key.substring(PREFIX_ENV.length() + 1), value);
                } else {
                    getSparkProperties().setProperty(key.substring(PREFIX_ENV.length() + 1), value);
                }
        }
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return "SparkJobConfig{" +
                "clientSparkVersion='" + clientSparkVersion + '\'' +
                ", action='" + action + '\'' +
                ", appResource='" + appResource + '\'' +
                ", mainClass='" + mainClass + '\'' +
                ", appArgs=" + Arrays.toString(appArgs) +
                ", sparkProperties=" + sparkProperties +
                ", environmentVariables=" + environmentVariables +
                ", appType='" + appType + '\'' +
                ", logDir='" + logDir + '\'' +
                ", monitoringInterval=" + monitoringInterval +
                '}';
    }
}
