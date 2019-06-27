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
import com.cognitree.kronos.model.TaskId;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockTaskHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(MockTaskHandler.class);

    private static final List<TaskId> tasks = Collections.synchronizedList(new ArrayList<>());

    private Task task;

    @Override
    public void init(Task task, ObjectNode config) {
        this.task = task;
    }

    @Override
    public TaskResult execute() {
        logger.info("Received request to execute task {}", task);

        while (!tasks.contains(task)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                logger.error("Thread has been interrupted");
            }
        }
        tasks.remove(task);
        return TaskResult.SUCCESS;
    }

    @Override
    public void abort() {
        tasks.add(task);
    }
}
