package com.cognitree.kronos.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

public class Namespace {
    private String name;
    private String description;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonIgnore
    public String getIdentity() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Namespace)) return false;
        Namespace namespace = (Namespace) o;
        return Objects.equals(name, namespace.name) &&
                Objects.equals(description, namespace.description);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, description);
    }

    @Override
    public String toString() {
        return "Namespace{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
