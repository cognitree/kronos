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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.cognitree.kronos.model.Task.Status.CREATED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.WAITING;

public class TestTaskStatusChangeListener implements TaskStatusChangeListener {
    private Map<Task, Status> TASK_STATUS_MAP = Collections.synchronizedMap(new HashMap<>());

    public boolean isStatusReceived(Task task) {
        return TASK_STATUS_MAP.containsKey(task);
    }

    @Override
    public void statusChanged(Task task, Status from, Status to) {
        // initially all tasks will be in created state
        // update the task status map with created state if task status change notification is received for the first time
        if (!TASK_STATUS_MAP.containsKey(task)) {
            TASK_STATUS_MAP.put(task, CREATED);
        }
        final Status lastKnownStatus = TASK_STATUS_MAP.get(task);
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
                if (lastKnownStatus != RUNNING) {
                    Assert.fail("invalid task status change notification");
                }
                break;
            case FAILED:
                break;
        }
        TASK_STATUS_MAP.put(task, task.getStatus());
    }
}
