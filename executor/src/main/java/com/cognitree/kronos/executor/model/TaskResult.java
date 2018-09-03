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

package com.cognitree.kronos.executor.model;

import java.util.Map;
import java.util.Objects;

public class TaskResult {
    public static final TaskResult SUCCESS = new TaskResult(true);

    private boolean success;
    private String message;
    private Map<String, Object> context;

    public TaskResult(boolean success) {
        this(success, null, null);
    }

    public TaskResult(boolean success, String message) {
        this(success, message, null);
    }

    public TaskResult(boolean success, String message, Map<String, Object> context) {
        this.success = success;
        this.message = message;
        this.context = context;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskResult)) return false;
        TaskResult that = (TaskResult) o;
        return success == that.success &&
                Objects.equals(message, that.message) &&
                Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {

        return Objects.hash(success, message, context);
    }

    @Override
    public String toString() {
        return "TaskResult{" +
                "success=" + success +
                ", message='" + message + '\'' +
                ", context=" + context +
                '}';
    }
}