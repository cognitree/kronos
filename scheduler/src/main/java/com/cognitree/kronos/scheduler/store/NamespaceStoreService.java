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

import java.util.List;

public class NamespaceStoreService implements StoreService<Namespace, String> {

    private final NamespaceStoreConfig namespaceStoreConfig;
    private NamespaceStore namespaceStore;

    public NamespaceStoreService(NamespaceStoreConfig namespaceStoreConfig) {
        this.namespaceStoreConfig = namespaceStoreConfig;
    }

    public static NamespaceStoreService getService() {
        return (NamespaceStoreService) StoreServiceProvider.getStoreService(NamespaceStoreService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        namespaceStore = (NamespaceStore) Class.forName(namespaceStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        namespaceStore.init(namespaceStoreConfig.getConfig());
    }

    @Override
    public void start() {

    }

    public List<Namespace> load() {
        return namespaceStore.load();
    }

    public Namespace load(String namespaceId) {
        return namespaceStore.load(namespaceId);
    }

    public void store(Namespace namespace) {
        namespaceStore.store(namespace);
    }

    public void update(Namespace namespace) {
        namespaceStore.store(namespace);
    }

    public void delete(String namespaceId) {
        namespaceStore.delete(namespaceId);
    }

    @Override
    public void stop() {
        namespaceStore.stop();
    }

}
