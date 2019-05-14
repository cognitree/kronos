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

package com.cognitree.kronos.store;

import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class NamespaceStoreTest extends StoreTest {

    @Before
    public void setup() throws Exception {
        // initialize store with some namespaces
        for (int i = 0; i < 5; i++) {
            createNamespace(UUID.randomUUID().toString());
        }
    }

    @Test
    public void testStoreAndLoadNamespace() throws StoreException {
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        List<Namespace> existingNamespaces = namespaceStore.load();

        // test create and load
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        existingNamespaces.add(namespaceOne);

        Namespace namespaceTwo = createNamespace(UUID.randomUUID().toString());
        existingNamespaces.add(namespaceTwo);

        assertEquals(namespaceOne, namespaceStore.load(namespaceOne));
        assertEquals(namespaceTwo, namespaceStore.load(namespaceTwo));

        List<Namespace> expectedNamespaces = namespaceStore.load();
        Assert.assertTrue("Namespaces loaded from store do not match with expected",
                expectedNamespaces.size() == existingNamespaces.size() &&
                        expectedNamespaces.containsAll(existingNamespaces)
                        && existingNamespaces.containsAll(expectedNamespaces));
    }

    @Test
    public void testUpdateNamespace() throws StoreException {
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        List<Namespace> existingNamespaces = namespaceStore.load();
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        namespace.setDescription("updated namespace one");
        namespaceStore.update(namespace);
        assertEquals(namespace, namespaceStore.load(namespace));
        List<Namespace> namespacesPostUpdate = namespaceStore.load();
        namespacesPostUpdate.remove(namespace);
        Assert.assertTrue("Namespaces loaded from store do not match with expected",
                namespacesPostUpdate.size() == existingNamespaces.size() &&
                        namespacesPostUpdate.containsAll(existingNamespaces)
                        && existingNamespaces.containsAll(namespacesPostUpdate));
    }

    @Test
    public void testDeleteNamespace() throws StoreException {
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        List<Namespace> existingNamespaces = namespaceStore.load();
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        namespaceStore.delete(namespace);
        List<Namespace> namespacesPostDelete = namespaceStore.load();
        Assert.assertTrue("Namespaces loaded from store do not match with expected",
                namespacesPostDelete.size() == existingNamespaces.size() &&
                        namespacesPostDelete.containsAll(existingNamespaces)
                        && existingNamespaces.containsAll(namespacesPostDelete));

    }
}
