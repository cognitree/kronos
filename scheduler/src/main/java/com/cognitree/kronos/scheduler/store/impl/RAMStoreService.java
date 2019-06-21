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

package com.cognitree.kronos.scheduler.store.impl;

import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RAMStoreService extends StoreService {
    private static final Logger logger = LoggerFactory.getLogger(RAMStoreService.class);

    private NamespaceStore namespaceStore;
    private WorkflowStore workflowStore;
    private WorkflowTriggerStore workflowTriggerStore;
    private JobStore jobStore;
    private TaskStore taskStore;
    private org.quartz.spi.JobStore quartzJobStore;

    public RAMStoreService(ObjectNode config) {
        super(config);
    }

    @Override
    public void init() {
        logger.info("Initializing RAM store service");
        namespaceStore = new RAMNamespaceStore();
        workflowStore = new RAMWorkflowStore();
        workflowTriggerStore = new RAMWorkflowTriggerStore();
        jobStore = new RAMJobStore();
        taskStore = new RAMTaskStore();
        quartzJobStore = new org.quartz.simpl.RAMJobStore();
    }

    @Override
    public void start() {
        logger.info("Starting RAM store service");
    }

    @Override
    public NamespaceStore getNamespaceStore() {
        return namespaceStore;
    }

    @Override
    public WorkflowStore getWorkflowStore() {
        return workflowStore;
    }

    @Override
    public WorkflowTriggerStore getWorkflowTriggerStore() {
        return workflowTriggerStore;
    }

    @Override
    public JobStore getJobStore() {
        return jobStore;
    }

    @Override
    public TaskStore getTaskStore() {
        return taskStore;
    }

    @Override
    public org.quartz.spi.JobStore getQuartzJobStore() {
        return quartzJobStore;
    }

    @Override
    public void stop() {
        logger.info("Stopping RAM store service");
    }
}
