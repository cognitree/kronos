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

package com.cognitree.kronos.scheduler.policies;

import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;

import java.util.HashSet;
import java.util.Set;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.scheduler.model.Messages.TIMED_OUT;

public class MockTimeoutPolicy implements TimeoutPolicy {

    private static final Set<Task> timeoutTasks = new HashSet<>();

    public static Set<Task> getTimeoutTasks() {
        return timeoutTasks;
    }

    @Override
    public void init(ObjectNode policyConfig) {

    }

    @Override
    public void handle(Task task) {
        Assert.assertEquals(FAILED, task.getStatus());
        Assert.assertEquals(TIMED_OUT, task.getStatusMessage());
        timeoutTasks.add(task);
    }
}
