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

import com.cognitree.tasks.model.Task;
import com.cognitree.tasks.model.Task.Status;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static com.cognitree.tasks.model.FailureMessage.TIMED_OUT;
import static com.cognitree.tasks.model.Task.Status.FAILED;
import static com.cognitree.tasks.model.Task.Status.SUCCESSFUL;

/**
 * timeout policy which marks a tasks as {@link Status#FAILED} on timeout
 */
public class FailOnTimeoutPolicy implements TimeoutPolicy {

    private TimeoutAction timeoutAction;

    @Override
    public void init(ObjectNode policyConfig, TimeoutAction timeoutAction) {
        this.timeoutAction = timeoutAction;
    }

    @Override
    public void handle(Task task) {
        if (!(task.getStatus().equals(FAILED) || task.getStatus().equals(SUCCESSFUL))) {
            timeoutAction.updateTask(task, FAILED, TIMED_OUT);
        }
    }
}
