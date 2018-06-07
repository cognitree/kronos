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

package com.cognitree.kronos.queue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A factory class providing in-memory queue for a given type of data
 */
public class InMemoryQueueFactory {

    private static final Map<Object, LinkedBlockingQueue<String>> IN_MEMORY_QUEUE_MAP = new HashMap<>();

    public static LinkedBlockingQueue<String> getQueue(String topic) {
        if (!IN_MEMORY_QUEUE_MAP.containsKey(topic)) {
            createQueue(topic);
        }
        return IN_MEMORY_QUEUE_MAP.get(topic);
    }

    private synchronized static void createQueue(String topic) {
        if (!IN_MEMORY_QUEUE_MAP.containsKey(topic)) {
            IN_MEMORY_QUEUE_MAP.put(topic, new LinkedBlockingQueue<>());
        }
    }
}
