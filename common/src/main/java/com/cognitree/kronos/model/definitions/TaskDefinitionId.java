package com.cognitree.kronos.model.definitions;

public class TaskDefinitionId {

    protected String name;

    public static TaskDefinitionId create(String name) {
        final TaskDefinitionId taskDefinitionId = new TaskDefinitionId();
        taskDefinitionId.setName(name);
        return taskDefinitionId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
