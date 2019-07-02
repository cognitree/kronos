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

package com.cognitree.kronos.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

import static com.cognitree.kronos.model.Policy.Type.retry;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RetryPolicy extends Policy {
    private int maxRetryCount = 1;
    private boolean retryOnFailure = true;
    private boolean retryOnTimeout = false;

    public RetryPolicy() {
        super(retry);
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public boolean isRetryOnFailure() {
        return retryOnFailure;
    }

    public void setRetryOnFailure(boolean retryOnFailure) {
        this.retryOnFailure = retryOnFailure;
    }

    public boolean isRetryOnTimeout() {
        return retryOnTimeout;
    }

    public void setRetryOnTimeout(boolean retryOnTimeout) {
        this.retryOnTimeout = retryOnTimeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RetryPolicy)) return false;
        if (!super.equals(o)) return false;
        RetryPolicy that = (RetryPolicy) o;
        return maxRetryCount == that.maxRetryCount &&
                retryOnFailure == that.retryOnFailure &&
                retryOnTimeout == that.retryOnTimeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxRetryCount, retryOnFailure, retryOnTimeout);
    }

    @Override
    public String toString() {
        return "RetryPolicy{" +
                "maxRetryCount=" + maxRetryCount +
                ", retryOnFailure=" + retryOnFailure +
                ", retryOnTimeout=" + retryOnTimeout +
                "} " + super.toString();
    }
}
