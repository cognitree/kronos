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

import java.util.Objects;

public class JobResponse {
    private Action action;
    private String serverSparkVersion;
    private String submissionId;
    private Boolean success;

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public String getServerSparkVersion() {
        return serverSparkVersion;
    }

    public void setServerSparkVersion(String serverSparkVersion) {
        this.serverSparkVersion = serverSparkVersion;
    }

    public String getSubmissionId() {
        return submissionId;
    }

    public void setSubmissionId(String submissionId) {
        this.submissionId = submissionId;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobResponse)) return false;
        JobResponse that = (JobResponse) o;
        return action == that.action &&
                Objects.equals(serverSparkVersion, that.serverSparkVersion) &&
                Objects.equals(submissionId, that.submissionId) &&
                Objects.equals(success, that.success);
    }

    @Override
    public int hashCode() {

        return Objects.hash(action, serverSparkVersion, submissionId, success);
    }

    @Override
    public String toString() {
        return "JobResponse{" +
                "action=" + action +
                ", serverSparkVersion='" + serverSparkVersion + '\'' +
                ", submissionId='" + submissionId + '\'' +
                ", success=" + success +
                '}';
    }
}
