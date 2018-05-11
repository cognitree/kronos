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

public class SparkCluster {
    private String master;
    private String restBaseURL;
    private String clientSparkVersion;
    private String sparkHomeDir;

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getRestBaseURL() {
        return restBaseURL;
    }

    public void setRestBaseURL(String restBaseURL) {
        this.restBaseURL = restBaseURL;
    }

    public String getClientSparkVersion() {
        return clientSparkVersion;
    }

    public void setClientSparkVersion(String clientSparkVersion) {
        this.clientSparkVersion = clientSparkVersion;
    }

    public String getSparkHomeDir() {
        return sparkHomeDir;
    }

    public void setSparkHomeDir(String sparkHomeDir) {
        this.sparkHomeDir = sparkHomeDir;
    }

    public void set(String key, String value) {
        switch (key) {
            case "master":
                setMaster(value);
                break;
            case "restBaseURL":
                setRestBaseURL(value);
                break;
            case "sparkHomeDir":
                setRestBaseURL(value);
                break;
            case "clientSparkVersion":
                setClientSparkVersion(value);
                break;
        }
    }

    @Override
    public String toString() {
        return "SparkCluster{" +
                "master='" + master + '\'' +
                ", restBaseURL='" + restBaseURL + '\'' +
                ", clientSparkVersion='" + clientSparkVersion + '\'' +
                ", sparkHomeDir='" + sparkHomeDir + '\'' +
                '}';
    }
}
