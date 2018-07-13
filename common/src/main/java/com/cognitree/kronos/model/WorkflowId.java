package com.cognitree.kronos.model;

public class WorkflowId {

    protected String id;
    protected String namespace;

    WorkflowId() {
    }

    public static WorkflowId create(String id, String namespace) {
        final WorkflowId workflowId = new WorkflowId();
        workflowId.setId(id);
        workflowId.setNamespace(namespace);
        return workflowId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
