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

package com.cognitree.examples.tasks.executor.handlers;

import com.cognitree.tasks.executor.TaskStatusListener;
import com.cognitree.tasks.executor.handlers.TaskHandler;
import com.cognitree.tasks.model.Task;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

import static com.cognitree.tasks.model.Task.Status.SUCCESSFUL;

/**
 * writes a tasks into a file and sends back status as SUCCESSFUL
 */
public class FileLogger implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(FileLogger.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String fileName = System.getProperty("java.io.tmpdir") + "/tasks.txt";
    private TaskStatusListener statusListener;
    private FileWriter fileWriter;

    @Override
    public void init(ObjectNode handlerConfig, TaskStatusListener statusListener) {
        this.statusListener = statusListener;
        try {
            fileWriter = new FileWriter(fileName, true);
        } catch (Exception e) {
            // do nothing
        }
    }


    @Override
    public void handleTask(Task task) throws IOException {
        logger.info("Received request to handle task {}", task);
        fileWriter.write(OBJECT_MAPPER.writeValueAsString(task));
        fileWriter.write("\n");
        fileWriter.flush();
        statusListener.updateStatus(task.getId(), task.getGroup(), SUCCESSFUL);
    }
}
