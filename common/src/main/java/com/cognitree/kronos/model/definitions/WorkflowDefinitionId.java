package com.cognitree.kronos.model.definitions;

public class WorkflowDefinitionId {

    protected String name;
    protected String namespace;

    public static WorkflowDefinitionId create(String name, String namespace) {
        final WorkflowDefinitionId workflowDefinitionId = new WorkflowDefinitionId();
        workflowDefinitionId.setName(name);
        workflowDefinitionId.setNamespace(namespace);
        return workflowDefinitionId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
