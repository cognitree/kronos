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

public class JobSubmitResponse extends JobResponse {

    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobSubmitResponse)) return false;
        if (!super.equals(o)) return false;
        JobSubmitResponse that = (JobSubmitResponse) o;
        return Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), message);
    }

    @Override
    public String toString() {
        return "JobSubmitResponse{" +
                "message='" + message + '\'' +
                "} " + super.toString();
    }
}