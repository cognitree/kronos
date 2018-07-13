package com.cognitree.kronos.model;

public class MutableTaskId implements TaskId {

    private String id;
    private String workflowId;
    private String namespace;

    protected MutableTaskId() {
    }

    public static TaskId create(String id, String workflowId, String namespace) {
        final MutableTaskId mutableTaskId = new MutableTaskId();
        mutableTaskId.setId(id);
        mutableTaskId.setWorkflowId(workflowId);
        mutableTaskId.setNamespace(namespace);
        return mutableTaskId;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
