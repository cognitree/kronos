package com.cognitree.kronos.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Namespace extends NamespaceId {
    private String description;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonIgnore
    public NamespaceId getIdentity() {
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
        return "Namespace{" +
                "description='" + description + '\'' +
                "} " + super.toString();
    }
}
