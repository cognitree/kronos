package com.cognitree.kronos.model.definitions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = WorkflowTrigger.class)
@JsonDeserialize(as = WorkflowTrigger.class)
public class WorkflowTrigger extends WorkflowTriggerId {
    private long startAt;
    private String schedule;
    private long endAt;
    private boolean isEnabled = true;

    public long getStartAt() {
        return startAt;
    }

    public void setStartAt(long startAt) {
        this.startAt = startAt;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public Long getEndAt() {
        return endAt;
    }

    public void setEndAt(Long endAt) {
        this.endAt = endAt;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
    }

    @JsonIgnore
    public WorkflowTriggerId getIdentity() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {

        return super.hashCode();
    }

    @Override
    public String toString() {
        return "WorkflowTrigger{" +
                "startAt=" + startAt +
                ", schedule='" + schedule + '\'' +
                ", endAt=" + endAt +
                ", isEnabled=" + isEnabled +
                "} " + super.toString();
    }
}