package com.cognitree.kronos.response;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Objects;

@JsonSerialize(as = WorkflowResponse.class)
@JsonDeserialize(as = WorkflowResponse.class)
public class WorkflowResponse extends Workflow {
    private List<Task> tasks;

    public static WorkflowResponse create(Workflow workflow, List<Task> tasks) {
        WorkflowResponse workflowResponse = new WorkflowResponse();
        workflowResponse.setTasks(tasks);
        workflowResponse.setId(workflow.getId());
        workflowResponse.setName(workflow.getName());
        workflowResponse.setNamespace(workflow.getNamespace());
        workflowResponse.setStatus(workflow.getStatus());
        workflowResponse.setCreatedAt(workflow.getCreatedAt());
        workflowResponse.setCompletedAt(workflow.getCompletedAt());
        return workflowResponse;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowResponse)) return false;
        if (!super.equals(o)) return false;
        WorkflowResponse that = (WorkflowResponse) o;
        return Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), tasks);
    }

    @Override
    public String toString() {
        return "WorkflowResponse{" +
                "tasks=" + tasks +
                "} " + super.toString();
    }
}
