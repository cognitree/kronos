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

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 *
 * There must be only one implementation of this service registered with the {@link ServiceProvider} by calling
 * {@link ServiceProvider#registerService(Service)}.
 */
public abstract class StoreService implements Service {

    protected ObjectNode config;

    public StoreService(ObjectNode config) {
        this.config = config;
        ServiceProvider.registerService(this);
    }

    @Override
    public final String getName() {
        return StoreService.class.getSimpleName();
    }

    public abstract NamespaceStore getNamespaceStore();
    public abstract WorkflowStore getWorkflowStore();
    public abstract WorkflowTriggerStore getWorkflowTriggerStore();
    public abstract JobStore getJobStore();
    public abstract TaskStore getTaskStore();
    public abstract org.quartz.spi.JobStore getQuartzJobStore();
}
