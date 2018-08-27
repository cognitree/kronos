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

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

import static com.cognitree.kronos.model.Task.Status.CREATED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.WAITING;

public class MockTaskStatusChangeListener implements TaskStatusChangeListener {
    private Map<String, Status> taskStatusMap = new HashMap<>();

    @Override
    public void statusChanged(Task task, Status from, Status to) {
        // initially all tasks will be in created state
        // update the task status map with created state if task status change notification is received for the first time
        if (!taskStatusMap.containsKey(task.getName())) {
            taskStatusMap.put(task.getName(), CREATED);
        }
        final Status lastKnownStatus = taskStatusMap.get(task.getName());
        switch (to) {
            case WAITING:
                if (lastKnownStatus != CREATED) {
                    Assert.fail("invalid task status change notification");
                }
                break;
            case SCHEDULED:
                if (lastKnownStatus != WAITING) {
                    Assert.fail("invalid task status change notification");
                }
                break;
            case SUBMITTED:
                if (lastKnownStatus != SCHEDULED) {
                    Assert.fail("invalid task status change notification");
                }
                break;
            case RUNNING:
                if (lastKnownStatus != SUBMITTED) {
                    Assert.fail("invalid task status change notification");
                }
                break;
            case SUCCESSFUL:
                taskStatusMap.remove(task.getName());
                if (lastKnownStatus != RUNNING) {
                    Assert.fail("invalid task status change notification");
                }
                break;
            case FAILED:
                taskStatusMap.remove(task.getName());
                // task can be marked failed from any of the above state
                break;
        }
        if (!task.getStatus().isFinal())
            taskStatusMap.put(task.getName(), to);
    }
}
