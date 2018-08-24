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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreConfig;

import java.util.List;

public class NamespaceService implements Service {

    private final StoreConfig storeConfig;
    private NamespaceStore namespaceStore;

    public NamespaceService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public static NamespaceService getService() {
        return (NamespaceService) ServiceProvider.getService(NamespaceService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        namespaceStore = (NamespaceStore) Class.forName(storeConfig.getStoreClass())
                .getConstructor().newInstance();
        namespaceStore.init(storeConfig.getConfig());
    }

    @Override
    public void start() {

    }

    public List<Namespace> get() {
        return namespaceStore.load();
    }

    public Namespace get(String namespaceId) {
        return namespaceStore.load(namespaceId);
    }

    public void add(Namespace namespace) {
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
