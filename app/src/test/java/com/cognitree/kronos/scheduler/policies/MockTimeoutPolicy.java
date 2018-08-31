package com.cognitree.kronos.scheduler.policies;

import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;

import java.util.HashSet;
import java.util.Set;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.util.Messages.TIMED_OUT;

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
