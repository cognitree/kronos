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

import com.cognitree.kronos.executor.TaskExecutionService;
import com.cognitree.kronos.scheduler.TaskReaderService;
import com.cognitree.kronos.scheduler.TaskSchedulerService;

import java.util.HashMap;
import java.util.Map;

/**
 * A registry used to register and lookup a {@link Service} implementation.
 */
public class ServiceProvider {
    private static final Map<String, Service> serviceMap = new HashMap<>();

    public static void registerService(Service service) {
        serviceMap.put(service.getName(), service);
    }

    public static Service getService(String name) {
        return serviceMap.get(name);
    }

    public static TaskReaderService getTaskReaderService() {
        return (TaskReaderService) serviceMap.get(TaskReaderService.class.getSimpleName());
    }

    public static TaskSchedulerService getTaskSchedulerService() {
        return (TaskSchedulerService) serviceMap.get(TaskSchedulerService.class.getSimpleName());
    }

    public static TaskExecutionService getTaskExecutionService() {
        return (TaskExecutionService) serviceMap.get(TaskExecutionService.class.getSimpleName());
    }
}
