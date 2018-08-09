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

public class JobStatusResponse extends JobResponse {

    private DriverState driverState;
    private String workerHostPort;
    private String workerId;

    public DriverState getDriverState() {
        return driverState;
    }

    public void setDriverState(DriverState driverState) {
        this.driverState = driverState;
    }

    public String getWorkerHostPort() {
        return workerHostPort;
    }

    public void setWorkerHostPort(String workerHostPort) {
        this.workerHostPort = workerHostPort;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobStatusResponse)) return false;
        if (!super.equals(o)) return false;
        JobStatusResponse that = (JobStatusResponse) o;
        return driverState == that.driverState &&
                Objects.equals(workerHostPort, that.workerHostPort) &&
                Objects.equals(workerId, that.workerId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), driverState, workerHostPort, workerId);
    }

    @Override
    public String toString() {
        return "JobStatusResponse{" +
                "driverState=" + driverState +
                ", workerHostPort='" + workerHostPort + '\'' +
                ", workerId='" + workerId + '\'' +
                "} " + super.toString();
    }

    public enum DriverState {
        // SUBMITTED: Submitted but not yet scheduled on a worker
        // RUNNING: Has been allocated to a worker to run,
        // FINISHED: Previously ran and exited cleanly
        // RELAUNCHING: Exited non-zero or due to worker failure, but has not yet started running again
        // UNKNOWN: The state of the driver is temporarily not known due to master failure recovery
        // KILLED: A user manually killed this driver
        // FAILED: The driver exited non-zero and was not supervised
        // ERROR: Unable to run or restart due to an unrecoverable error (e.g. missing jar file)
        SUBMITTED(false),
        RUNNING(false),
        FINISHED(true),
        RELAUNCHING(false),
        UNKNOWN(false),
        KILLED(true),
        FAILED(true),
        ERROR(true);

        private final boolean isFinal;

        DriverState(boolean isFinal) {
            this.isFinal = isFinal;
        }

        public boolean isFinal() {
            return this.isFinal;
        }
    }
}