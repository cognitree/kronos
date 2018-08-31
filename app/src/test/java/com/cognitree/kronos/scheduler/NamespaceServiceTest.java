package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.ApplicationTest;
import com.cognitree.kronos.model.Namespace;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class NamespaceServiceTest extends ApplicationTest {

    @Test
    public void testAddNamespace() throws ServiceException {
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

    @Test(expected = ServiceException.class)
    public void testReAddNamespace() throws ServiceException {
        final NamespaceService namespaceService = NamespaceService.getService();
        Namespace namespace = createNamespace(UUID.randomUUID().toString());
        namespaceService.add(namespace);
        namespaceService.add(namespace);
        Assert.fail();
    }

    @Test
    public void testUpdateNamespace() throws ServiceException {
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
