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

package com.cognitree.kronos.scheduler;

public enum ValidationError {
    NAMESPACE_NOT_FOUND(1001, "namespace_not_found", 404),
    NAMESPACE_ALREADY_EXISTS(1002, "namespace_already_exists", 409),

    WORKFLOW_NOT_FOUND(2003, "workflow_not_found", 404),
    WORKFLOW_ALREADY_EXISTS(2003, "workflow_already_exists", 409),
    MISSING_TASK_IN_WORKFLOW(2001, "missing_tasks_in_workflow", 400),
    CYCLIC_DEPENDENCY_IN_WORKFLOW(2003, "cyclic_dependency_in_workflow", 400),

    WORKFLOW_TRIGGER_NOT_FOUND(3001, "workflow_trigger_not_found", 404),
    INVALID_WORKFLOW_TRIGGER(3002, "invalid_workflow_trigger", 400),
    WORKFLOW_TRIGGER_ALREADY_EXISTS(3003, "workflow_trigger_already_exists", 404);

    private int errorCode;
    private String errorMsg;
    private int statusCode;

    ValidationError(int errorCode, String errorMsg, int statusCode) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
        this.statusCode = statusCode;
    }

    public ValidationException createException(Object... args) {
        return new ValidationException(this, args);
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public String toString() {
        return "ValidationError{" +
                "errorCode=" + errorCode +
                ", errorMsg='" + errorMsg + '\'' +
                ", statusCode=" + statusCode +
                "} " + super.toString();
    }
}
