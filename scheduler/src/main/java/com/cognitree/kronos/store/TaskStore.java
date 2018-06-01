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

package com.cognitree.kronos.store;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * An interface exposing API's to provide {@link Task} persistence
 */
public interface TaskStore {

    /**
     * called during initialization phase to initialize the task store using {@link TaskStoreConfig#config}
     *
     * @param storeConfig
     * @throws Exception
     */
    void init(ObjectNode storeConfig) throws Exception;

    void store(Task task);

    void update(String taskId, String taskGroup, Status status, String statusMessage, long submittedAt, long completedAt);

    Task load(String taskId, String taskGroup);

    List<Task> load(List<Status> statuses);

    List<Task> load(String taskName, String taskGroup, long createdBefore, long createdAfter);

    void stop();
}