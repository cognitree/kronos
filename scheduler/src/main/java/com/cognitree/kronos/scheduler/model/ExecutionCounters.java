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

package com.cognitree.kronos.scheduler.model;

public class ExecutionCounters {
    private Integer total;
    private Integer active;
    private Integer successful;
    private Integer failed;
    private Integer skipped;
    private Integer stopped;

    public Integer getActive() {
        return active;
    }

    public void setActive(Integer active) {
        this.active = active;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getSuccessful() {
        return successful;
    }

    public void setSuccessful(Integer successful) {
        this.successful = successful;
    }

    public Integer getFailed() {
        return failed;
    }

    public void setFailed(Integer failed) {
        this.failed = failed;
    }

    public Integer getSkipped() {
        return skipped;
    }

    public void setSkipped(Integer skipped) {
        this.skipped = skipped;
    }

    public Integer getStopped() {
        return stopped;
    }

    public void setStopped(Integer stopped) {
        this.stopped = stopped;
    }

    @Override
    public String toString() {
        return "ExecutionCounters{" +
                "total=" + total +
                ", active=" + active +
                ", successful=" + successful +
                ", failed=" + failed +
                ", skipped=" + skipped +
                ", stopped=" + stopped +
                '}';
    }
}
