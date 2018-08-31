/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.NamespaceId;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryNamespaceStore implements NamespaceStore {

    private final Map<NamespaceId, Namespace> namespaces = new HashMap<>();

    @Override
    public void init(ObjectNode storeConfig) {

    }

    @Override
    public List<Namespace> load() {
        return new ArrayList<>(namespaces.values());
    }

    @Override
    public void store(Namespace namespace) throws StoreException {
        final NamespaceId namespaceId = Namespace.build(namespace.getName());
        if (namespaces.containsKey(namespaceId)) {
            throw new StoreException("already exists");
        }
        namespaces.put(namespaceId, namespace);
    }

    @Override
    public Namespace load(NamespaceId namespaceId) {
        return namespaces.get(Namespace.build(namespaceId.getName()));
    }

    @Override
    public void update(Namespace namespace) throws StoreException {
        final NamespaceId namespaceId = Namespace.build(namespace.getName());
        if (!namespaces.containsKey(namespaceId)) {
            throw new StoreException("does not exists");
        }
        namespaces.put(namespaceId, namespace);
    }

    @Override
    public void delete(NamespaceId namespaceId) throws StoreException {
        if (namespaces.remove(Namespace.build(namespaceId.getName())) == null) {
            throw new StoreException("does not exists");
        }
    }

    @Override
    public void stop() {

    }
}
