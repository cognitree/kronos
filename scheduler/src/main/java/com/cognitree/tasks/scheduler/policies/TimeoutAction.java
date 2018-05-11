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

/**
 * actions that can be taken on a task in case of timeout, used by {@link TimeoutPolicy} implementation to take actions
 * on task which have been timed out
 */
public interface TimeoutAction {
    /**
     * re add the task for execution
     *
     * @param task
     */
    void reAddTask(Task task);

    /**
     * updates task status
     *
     * @param task
     * @param status
     * @param statusMessage
     */
    void updateTask(Task task, Status status, String statusMessage);

    /**
     * updates task status
     *
     * @param task
     * @param status
     * @param statusMessage
     * @param runtimeProperties
     */
    void updateTask(Task task, Status status, String statusMessage, ObjectNode runtimeProperties);
}
