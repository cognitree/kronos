package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

import static com.cognitree.kronos.model.Task.Status.*;

public class MockTaskStatusChangeListener implements TaskStatusChangeListener {
    private Map<String, Status> taskStatusMap = new HashMap<>();

    @Override
    public void statusChanged(Task task, Status from, Status to) {
        // initially all tasks will be in created state
        // update the task status map with created state if task status change notification is received for the first time
        if (!taskStatusMap.containsKey(task.getId())) {
            taskStatusMap.put(task.getId(), CREATED);
        }
        final Status lastKnownStatus = taskStatusMap.get(task.getId());
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
                // task can be marked failed from any of the above state
                break;
        }
        taskStatusMap.put(task.getId(), to);
    }
}
