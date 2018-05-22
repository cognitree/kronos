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

import com.cognitree.kronos.executor.TaskStatusListener;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A handler interface to be implemented for each task type ({@link com.cognitree.kronos.model.TaskDefinition#type}).
 * <p>
 * A handler defines how a task of given type is handled/ executed and is responsible for notifying the task status back using
 * {@link TaskStatusListener#updateStatus(String, String, Task.Status)} OR
 * {@link TaskStatusListener#updateStatus(String, String, Task.Status, String)} OR
 * {@link TaskStatusListener#updateStatus(String, String, Task.Status, String, ObjectNode)} method.
 * </p>
 * Configuring a TaskHandler
 * <p>
 * A handler configuration is defined by {@link TaskHandlerConfig}.
 * A handler is initialized using {@link TaskHandler#init(ObjectNode, TaskStatusListener)} method
 * The {@link ObjectNode} argument is same as {@link TaskHandlerConfig#config} and is used by the handler to instantiate itself.
 * So any property required by the handler to instantiate itself should be part of {@link TaskHandlerConfig#config}
 * <p>
 * The (@link {@link TaskStatusListener} is the listener interested in the task status, any status update on task
 * should be notified back to the framework via {@link TaskStatusListener}
 * </p>
 */
public interface TaskHandler {

    /**
     * for each handler during initialization phase a call is made to initialize handler using {@link TaskHandlerConfig#config}
     * and a task status listener which should be used to notify about the task status
     *
     * @param handlerConfig  handler configuration used to initialize the handler
     * @param statusListener a task status listener used to notify about the task status
     */
    void init(ObjectNode handlerConfig, TaskStatusListener statusListener);

    /**
     * defines how a task is handled/ executed and is called whenever there is a task ready for execution
     * </p>
     *
     * @param task task to handle
     * @throws Exception
     */
    void handle(Task task) throws Exception;
}
