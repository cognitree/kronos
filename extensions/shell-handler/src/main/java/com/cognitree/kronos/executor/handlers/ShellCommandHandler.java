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
import com.cognitree.kronos.model.Task.TaskResult;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * A {@link TaskHandler} implementation to execute shell commands with given arguments.
 */
public class ShellCommandHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(ShellCommandHandler.class);

    private static final String PROP_CMD = "cmd";
    private static final String PROP_ARGS = "args";
    private static final String PROPERTY_WORKING_DIR = "workingDir";
    private static final String PROPERTY_LOG_DIR = "logDir";

    @Override
    public void init(ObjectNode handlerConfig) {
    }

    @Override
    public TaskResult handle(Task task) {
        logger.info("received request to handle task {}", task);

        final Map<String, Object> taskProperties = task.getProperties();
        if (!taskProperties.containsKey(PROP_CMD)) {
            return new TaskResult(false, "no command found to execute");
        }

        ArrayList<String> cmdWithArgs = new ArrayList<>();
        cmdWithArgs.add(getProperty(taskProperties, PROP_CMD));

        if (taskProperties.containsKey(PROP_ARGS)) {
            final String[] args = getProperty(taskProperties, PROP_ARGS, "").split(" ");
            cmdWithArgs.addAll(Arrays.asList(args));
        }

        ProcessBuilder processBuilder = new ProcessBuilder(cmdWithArgs);
        if (taskProperties.containsKey(PROPERTY_WORKING_DIR)) {
            processBuilder.directory(new File(getProperty(taskProperties, PROPERTY_WORKING_DIR)));
        }
        String logDirPath = getProperty(taskProperties, PROPERTY_LOG_DIR,
                System.getProperty("java.io.tmpdir"));
        File logDir = new File(logDirPath);
        // create log directory is does not exist
        if (!logDir.exists() && !logDir.mkdirs()) {
            return new TaskResult(false, "unable to create directory to store logs");
        }

        processBuilder.redirectError(new File(logDir, task.getName() + "_" + task.getJob() + "_stderr.log"));
        processBuilder.redirectOutput(new File(logDir, task.getName() + "_" + task.getJob() + "_stdout.log"));
        try {
            Process process = processBuilder.start();
            int exitValue = process.waitFor();
            logger.info("Process exited with code {} for command {}", exitValue, cmdWithArgs);
            if (exitValue != 0) {
                return new TaskResult(false, "process exited with error code " + exitValue);
            }
        } catch (Exception e) {
            logger.error("Error executing command {}", cmdWithArgs, e);
            return new TaskResult(false, "process exited with exception: " + e.getMessage());
        }
        return TaskResult.SUCCESS;
    }

    private String getProperty(Map<String, Object> properties, String key) {
        return String.valueOf(properties.get(key));
    }

    private String getProperty(Map<String, Object> properties, String key, String defaultValue) {
        return String.valueOf(properties.getOrDefault(key, defaultValue));
    }
}
