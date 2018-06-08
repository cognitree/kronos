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

package com.cognitree.kronos.scheduler.policies;

import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An interface implemented to define custom timeout policy to apply on a task in case of timeout.
 */
public interface TimeoutPolicy {

    /**
     * for each configured policy during initialization phase a call is made to initialize policy using
     * {@link TimeoutPolicyConfig#config}. Any property required by the timeout policy to instantiate itself
     * should be part of {@link TimeoutPolicyConfig#config}.
     *
     * @param policyConfig configuration used to initialize the policy.
     */
    void init(ObjectNode policyConfig);

    /**
     * called on task timeout.
     * <p>
     * attempts to modify the task instance, result in an UnsupportedOperationException
     *
     * @param task an unmodifiable instance of task.
     */
    void handle(Task task);
}
