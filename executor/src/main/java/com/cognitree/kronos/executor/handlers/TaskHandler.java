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

package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.executor.model.TaskResult;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow.WorkflowTask;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A handler defines how a task of given type is handled/ executed and is to be implemented and configured for each task type
 * ({@link WorkflowTask#type}).
 */
public interface TaskHandler {

    /**
     * for each configured handler during initialization phase a call is made to initialize handler using
     * {@link TaskHandlerConfig#config}. Any property required by the handler to instantiate itself
     * should be part of {@link TaskHandlerConfig#config}.
     *
     * @param handlerConfig configuration used to initialize the handler.
     */
    void init(ObjectNode handlerConfig);

    /**
     * defines how to handle/ execute the task.
     *
     * @param task task to handle.
     */
    TaskResult handle(Task task);
}
