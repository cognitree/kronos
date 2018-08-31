package com.cognitree.kronos.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = WorkflowTrigger.class)
@JsonDeserialize(as = WorkflowTrigger.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowTrigger extends WorkflowTriggerId {
    private Long startAt;
    private String schedule;
    private Long endAt;
    private boolean isEnabled = true;

    public Long getStartAt() {
        return startAt;
    }

    public void setStartAt(Long startAt) {
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