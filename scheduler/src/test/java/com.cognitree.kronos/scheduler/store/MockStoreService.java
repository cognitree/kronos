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

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.ServiceProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.quartz.simpl.RAMJobStore;

public class MockStoreService extends StoreService {
    private static final MockWorkflowStore WORKFLOW_STORE = new MockWorkflowStore();
    private static final MockNamespaceStore NAMESPACE_STORE = new MockNamespaceStore();
    private static final MockWorkflowTriggerStore WORKFLOW_TRIGGER_STORE = new MockWorkflowTriggerStore();
    private static final MockJobStore JOB_STORE = new MockJobStore();
    private static final MockTaskStore TASK_STORE = new MockTaskStore();
    private static final RAMJobStore RAM_JOB_STORE = new RAMJobStore();

    public MockStoreService(ObjectNode config) {
        super(config);
    }

    @Override
    public void init() {

    }

    @Override
    public void start() {
        ServiceProvider.registerService(this);
    }

    @Override
    public NamespaceStore getNamespaceStore() {
        return NAMESPACE_STORE;
    }

    @Override
    public WorkflowStore getWorkflowStore() {
        return WORKFLOW_STORE;
    }

    @Override
    public WorkflowTriggerStore getWorkflowTriggerStore() {
        return WORKFLOW_TRIGGER_STORE;
    }

    @Override
    public JobStore getJobStore() {
        return JOB_STORE;
    }

    @Override
    public TaskStore getTaskStore() {
        return TASK_STORE;
    }

    @Override
    public org.quartz.spi.JobStore getQuartzJobStore() {
        return RAM_JOB_STORE;
    }

    @Override
    public void stop() {

    }
}
