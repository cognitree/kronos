package com.cognitree.kronos.model;

import java.util.Objects;

public class NamespaceId {
    private String name;

    public static NamespaceId build(String name) {
        final NamespaceId namespaceId = new NamespaceId();
        namespaceId.setName(name);
        return namespaceId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NamespaceId)) return false;
        NamespaceId that = (NamespaceId) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "NamespaceId{" +
                "name='" + name + '\'' +
                '}';
    }
}
