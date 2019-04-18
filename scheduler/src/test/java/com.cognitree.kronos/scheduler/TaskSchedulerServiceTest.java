package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.queue.QueueConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TaskSchedulerServiceTest {
    private TaskSchedulerService taskSchedulerService;

    @Before
    public void setup() {
        taskSchedulerService = new TaskSchedulerService(new QueueConfig());
    }

    @Test
    public void updateTaskProperties() {
        Task task = new Task();
        Map<String, Object> properties = new HashMap<>();
        properties.put("namespace", "${taskA.namespace}");
        properties.put("logDir", "/tmp");
        Map<String, Object> values = new HashMap<>();
        values.put("type", "content");
        values.put("start", "${taskA.start}");
        values.put("end", "${*.end}");
        properties.put("values", values);
        task.setNamespace("default");
        task.setJob("jobA");
        task.setName("taskC");
        task.setType("testType");
        task.setProperties(properties);

        Map<String, Object> dependentTaskContext = new HashMap<>();
        dependentTaskContext.put("taskA.namespace", "default");
        dependentTaskContext.put("taskA.start", 1230);
        dependentTaskContext.put("taskB.end", 1240);

        taskSchedulerService.updateTaskProperties(task, dependentTaskContext);

        Assert.assertEquals(3, task.getProperties().size());
        Assert.assertEquals("default", task.getProperties().get("namespace"));
        Assert.assertEquals(3, ((Map<String, Object>) task.getProperties().get("values")).size());
        Assert.assertEquals(1230, (((Map<String, Object>) task.getProperties().get("values")).get("start")));
        Assert.assertEquals(1240, (((Map<String, Object>) task.getProperties().get("values")).get("end")));

    }
}