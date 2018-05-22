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

package com.cognitree.tasks.scheduler.policies;

import com.cognitree.tasks.executor.handlers.TaskHandlerConfig;
import com.cognitree.tasks.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An interface to be implemented by timeout policies which will be invoked when a task exceeds the max execution time
 * <p>
 * For details refer: {@link TaskHandlerConfig#maxExecutionTime}
 */
public interface TimeoutPolicy {

    /**
     * for each configured policy during initialization phase a call is made to initialize policy
     * using {@link TimeoutPolicyConfig#config}
     *
     * @param policyConfig configuration used to initialize the policy
     */
    void init(ObjectNode policyConfig);

    void handle(Task task);
}
