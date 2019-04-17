package com.cognitree.kronos.scheduler.events;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

/**
 * A model class to represent a config update event.
 */
public class ConfigUpdate {

    public enum Action { create, update, delete }

    private Action action;

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonSubTypes(value = {
            @JsonSubTypes.Type(Job.class),
            @JsonSubTypes.Type(Namespace.class),
            @JsonSubTypes.Type(Workflow.class),
            @JsonSubTypes.Type(WorkflowTrigger.class)
    })
    private Object model;

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public Object getModel() {
        return model;
    }

    public void setModel(Object model) {
        this.model = model;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigUpdate that = (ConfigUpdate) o;
        return action == that.action &&
                Objects.equals(model, that.model);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, model);
    }

    @Override
    public String toString() {
        return "ConfigUpdate{" +
                "action=" + action +
                ", model=" + model +
                '}';
    }
}
