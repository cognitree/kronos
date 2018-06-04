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

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskDefinition;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A handler defines how a task of given type is handled/ executed and is to be implemented for each task type
 * ({@link TaskDefinition#type}).
 * <p>
 * Configuring a TaskHandler
 * <p>
 * A handler configuration is defined by {@link TaskHandlerConfig}.
 * A handler is initialized using {@link TaskHandler#init(ObjectNode)} method. The {@link ObjectNode}
 * argument is same as {@link TaskHandlerConfig#config} and is used by the handler to instantiate itself. So any property
 * required by the handler to instantiate itself should be part of {@link TaskHandlerConfig#config}.
 * </p>
 */
public interface TaskHandler {

    /**
     * initialize the handler
     * <p>
     * invoked by framework during initialization phase made to initialize handler with {@link TaskHandlerConfig#config}.
     *
     * @param handlerConfig handler configuration used to initialize the handler.
     */
    void init(ObjectNode handlerConfig);

    /**
     * defines how to handle/ execute the task. If the execution succeeds without any exception then the task is
     * marked successful otherwise failed with status message same as exception message.
     *
     * @param task task to handle.
     * @throws HandlerException thrown if handler fails to execute the task successfully.
     */
    void handle(Task task) throws HandlerException;
}
