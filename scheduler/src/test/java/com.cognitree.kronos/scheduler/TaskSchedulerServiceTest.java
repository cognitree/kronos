package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.queue.QueueConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TaskSchedulerServiceTest {
    TaskSchedulerService taskSchedulerService;

    @Before
    public void setup() {
        taskSchedulerService = new TaskSchedulerService(new QueueConfig());
    }

    @Test
    public void updateTaskProperties() {
        Task task = new Task();

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> values = new HashMap<>();

        values.put("image", "index");
        values.put("tenantId", "123");
        values.put("type", "content");
        values.put("start", "${*.start}");
        values.put("end", "${*.end}");

        properties.put("namespace", "${A.namespace}");
        properties.put("logDir", "/tmp");
        properties.put("values", values);

        task.setNamespace("testspace");
        task.setJob("myjob");
        task.setName("content");
        task.setType("command");
        task.setProperties(properties);

        Map<String, Object> dependentTaskContext = new HashMap<>();
        dependentTaskContext.put("A.namespace", "orch");
        dependentTaskContext.put("A.start", 1230);
        dependentTaskContext.put("B.end", 1240);

        taskSchedulerService.updateTaskProperties(task, dependentTaskContext);

        Assert.assertEquals(3, task.getProperties().size());
        Assert.assertEquals("orch", task.getProperties().get("namespace"));
        Assert.assertEquals(5, ((Map<String, Object>) task.getProperties().get("values")).size());
        Assert.assertEquals(1230, (((Map<String, Object>) task.getProperties().get("values")).get("start")));
        Assert.assertEquals(1240, (((Map<String, Object>) task.getProperties().get("values")).get("end")));

    }
}