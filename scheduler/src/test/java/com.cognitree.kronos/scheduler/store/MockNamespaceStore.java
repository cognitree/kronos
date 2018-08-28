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

import java.util.Collections;
import java.util.List;

public class MockNamespaceStore implements NamespaceStore {

    @Override
    public void init(ObjectNode storeConfig) {

    }

    @Override
    public List<Namespace> load() {
        return Collections.emptyList();
    }

    @Override
    public void store(Namespace entity) {

    }

    @Override
    public Namespace load(NamespaceId identity) {
        return null;
    }

    @Override
    public void update(Namespace entity) {

    }

    @Override
    public void delete(NamespaceId identity) {

    }

    @Override
    public void stop() {

    }
}
