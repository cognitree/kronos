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

package com.cognitree.kronos.executor;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A task status listener interface listening for task status update
 */
public interface TaskStatusListener {

    /**
     * update status of task with id {@code taskId), group {@code taskGroup} to {@code status}
     *
     * @param taskId
     * @param taskGroup
     * @param status
     */
    void updateStatus(String taskId, String taskGroup, Status status);

    /**
     * update status of task with id {@code taskId), group {@code taskGroup} to {@code status}
     * with message {@code statusMessage}
     *
     * @param taskId
     * @param taskGroup
     * @param status
     * @param statusMessage
     */
    void updateStatus(String taskId, String taskGroup, Status status, String statusMessage);

    /**
     * update status of task with id {@code taskId), group {@code taskGroup} to {@code status}
     * with message {@code statusMessage}
     * <p>
     * {@code properties} is any additional properties or metadata to be associated with the task,
     * stored as {@link Task#runtimeProperties}
     * </p>
     *
     * @param taskId
     * @param taskGroup
     * @param status
     * @param statusMessage
     * @param properties
     */
    void updateStatus(String taskId, String taskGroup, Status status, String statusMessage, ObjectNode properties);
}
