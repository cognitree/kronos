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

import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.scheduler.model.Namespace;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static com.cognitree.kronos.TestUtil.createNamespace;

public class NamespaceServiceTest{
    private static final SchedulerApp SCHEDULER_APP = new SchedulerApp();
    private static final ExecutorApp EXECUTOR_APP = new ExecutorApp();

    @BeforeClass
    public static void start() throws Exception {
        SCHEDULER_APP.start();
        EXECUTOR_APP.start();
        // wait for the application to initialize itself
        Thread.sleep(100);
    }

    @AfterClass
    public static void stop() {
        SCHEDULER_APP.stop();
        EXECUTOR_APP.stop();
    }

    @Test
    public void testAddNamespace() throws ServiceException, ValidationException {
        final NamespaceService namespaceService = NamespaceService.getService();
        Namespace namespaceOne = createNamespace(UUID.randomUUID().toString());
        namespaceService.add(namespaceOne);

        final Namespace namespaceOneInDB = namespaceService.get(namespaceOne.getIdentity());
        Assert.assertNotNull(namespaceOneInDB);
        Assert.assertEquals(namespaceOne, namespaceOneInDB);

        Namespace namespaceTwo = createNamespace(UUID.randomUUID().toString());
        namespaceService.add(namespaceTwo);

        final Namespace namespaceTwoInDB = namespaceService.get(namespaceTwo.getIdentity());
        Assert.assertNotNull(namespaceTwoInDB);
        Assert.assertEquals(namespaceTwo, namespaceTwoInDB);

        final List<Namespace> namespaces = namespaceService.get();
        Assert.assertTrue(namespaces.contains(namespaceOneInDB));
        Assert.assertTrue(namespaces.contains(namespaceTwoInDB));
    }

    @Test(expected = ValidationException.class)
    public void testReAddNamespace() throws ServiceException, ValidationException {
        final NamespaceService namespaceService = NamespaceService.getService();
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        namespaceService.add(namespace);
        namespaceService.add(namespace);
        Assert.fail();
    }

    @Test
    public void testUpdateNamespace() throws ServiceException, ValidationException {
        final NamespaceService namespaceService = NamespaceService.getService();
        final String name = UUID.randomUUID().toString();
        Namespace namespace = createNamespace(name);
        namespaceService.add(namespace);

        Namespace updatedNamespace = createNamespace(name);
        updatedNamespace.setDescription("updated namespace");
        namespaceService.update(updatedNamespace);

        final Namespace namespacePostUpdate = namespaceService.get(updatedNamespace);
        Assert.assertNotNull(namespacePostUpdate);
        Assert.assertEquals(updatedNamespace, namespacePostUpdate);
    }
}
