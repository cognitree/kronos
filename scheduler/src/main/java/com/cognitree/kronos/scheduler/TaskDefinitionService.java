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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.cognitree.kronos.scheduler.store.StoreConfig;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.TaskDefinitionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TaskDefinitionService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskDefinitionService.class);

    private final StoreConfig storeConfig;
    private TaskDefinitionStore taskDefinitionStore;

    public TaskDefinitionService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static TaskDefinitionService getService() {
        return (TaskDefinitionService) ServiceProvider.getService(TaskDefinitionService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        logger.info("Initializing task definition service");
        taskDefinitionStore = (TaskDefinitionStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        taskDefinitionStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {
        logger.info("Starting task definition service");
    }

    public List<TaskDefinition> get() throws ServiceException {
        logger.debug("Received request to get all task definitions");
        try {
            return taskDefinitionStore.load();
        } catch (StoreException e) {
            logger.error("unable to get all task definitions", e);
            throw new ServiceException(e.getMessage());
        }
    }

    public TaskDefinition get(TaskDefinitionId taskDefinitionId) throws ServiceException {
        logger.debug("Received request to get task definition {}", taskDefinitionId);
        try {
            return taskDefinitionStore.load(taskDefinitionId);
        } catch (StoreException e) {
            logger.error("unable to get task definition {}", taskDefinitionId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void add(TaskDefinition taskDefinition) throws ServiceException {
        logger.debug("Received request to add task definition {}", taskDefinition);
        try {
            taskDefinitionStore.store(taskDefinition);
        } catch (StoreException e) {
            logger.error("unable to add task definition {}", taskDefinition, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void update(TaskDefinition taskDefinition) throws ServiceException {
        logger.debug("Received request to update task definition to {}", taskDefinition);
        try {
            taskDefinitionStore.store(taskDefinition);
        } catch (StoreException e) {
            logger.error("unable to update task definition to {}", taskDefinition, e);
            throw new ServiceException(e.getMessage());
        }
    }

    public void delete(TaskDefinitionId taskDefinitionId) throws ServiceException {
        logger.debug("Received request to delete task definition {}", taskDefinitionId);
        try {
            taskDefinitionStore.delete(taskDefinitionId);
        } catch (StoreException e) {
            logger.error("unable to delete task definition {}", taskDefinitionId, e);
            throw new ServiceException(e.getMessage());
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping task definition service");
        taskDefinitionStore.stop();
    }

}
