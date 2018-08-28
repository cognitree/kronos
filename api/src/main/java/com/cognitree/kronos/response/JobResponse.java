package com.cognitree.kronos.response;

import com.cognitree.kronos.model.Job;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Objects;

@JsonSerialize(as = JobResponse.class)
@JsonDeserialize(as = JobResponse.class)
public class JobResponse extends Job {
    private List<Task> tasks;

    public static JobResponse create(Job job, List<Task> tasks) {
        JobResponse jobResponse = new JobResponse();
        jobResponse.setTasks(tasks);
        jobResponse.setId(job.getId());
        jobResponse.setWorkflow(job.getWorkflow());
        jobResponse.setNamespace(job.getNamespace());
        jobResponse.setStatus(job.getStatus());
        jobResponse.setCreatedAt(job.getCreatedAt());
        jobResponse.setCompletedAt(job.getCompletedAt());
        return jobResponse;
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
        if (!(o instanceof JobResponse)) return false;
        if (!super.equals(o)) return false;
        JobResponse that = (JobResponse) o;
        return Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), tasks);
    }

    @Override
    public String toString() {
        return "JobResponse{" +
                "tasks=" + tasks +
                "} " + super.toString();
    }
}
