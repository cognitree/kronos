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

package com.cognitree.kronos.scheduler.store.impl;

import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RAMNamespaceStore implements NamespaceStore {
    private static final Logger logger = LoggerFactory.getLogger(RAMNamespaceStore.class);
    private final Map<NamespaceId, Namespace> namespaces = new HashMap<>();

    @Override
    public void init(ObjectNode storeConfig) {

    }

    @Override
    public void store(Namespace namespace) throws StoreException {
        logger.debug("Received request to store namespace {}", namespace);
        final NamespaceId namespaceId = Namespace.build(namespace.getName());
        if (namespaces.containsKey(namespaceId)) {
            throw new StoreException("namespace with id " + namespaceId + " already exists");
        }
        namespaces.put(namespaceId, namespace);
    }

    @Override
    public List<Namespace> load() {
        logger.debug("Received request to get all namespaces");
        return new ArrayList<>(namespaces.values());
    }

    @Override
    public Namespace load(NamespaceId namespaceId) {
        logger.debug("Received request to load namespace with id {}", namespaceId);
        return namespaces.get(Namespace.build(namespaceId.getName()));
    }

    @Override
    public void update(Namespace namespace) throws StoreException {
        logger.debug("Received request to update namespace to {}", namespace);
        final NamespaceId namespaceId = Namespace.build(namespace.getName());
        if (!namespaces.containsKey(namespaceId)) {
            throw new StoreException("namespace with id " + namespaceId + " does not exists");
        }
        namespaces.put(namespaceId, namespace);
    }

    @Override
    public void delete(NamespaceId namespaceId) throws StoreException {
        logger.debug("Received request to delete namespace with id {}", namespaceId);
        final NamespaceId builtNamespaceId = Namespace.build(namespaceId.getName());
        if (namespaces.remove(builtNamespaceId) == null) {
            throw new StoreException("namespace with id " + builtNamespaceId + " does not exists");
        }
    }

    @Override
    public void stop() {

    }
}
