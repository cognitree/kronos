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

package com.cognitree.kronos;

import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.scheduler.SchedulerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class which instantiates and starts scheduler and executor App
 */
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private final SchedulerApp schedulerApp;
    private final ExecutorApp executorApp;
    private final FileReader fileReader;

    public Application() {
        schedulerApp = new SchedulerApp();
        executorApp = new ExecutorApp();
        fileReader = new FileReader();
    }

    public static void main(String[] args) {
        Application application = new Application();
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
            application.init();
            application.start();
        } catch (Exception e) {
            logger.error("Error starting application", e);
            System.exit(0);
        }
    }

    public void init() throws Exception {
        logger.info("Initializing application");
        schedulerApp.init();
        executorApp.init();
    }

    public void start() throws Exception {
        logger.info("Starting application");
        schedulerApp.start();
        executorApp.start();
        fileReader.loadNamespaces();
        fileReader.loadWorkflows();
        fileReader.loadWorkflowTriggers();
    }

    public void stop() {
        logger.info("Stopping application");
        schedulerApp.stop();
        executorApp.stop();
    }
}
