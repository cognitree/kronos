package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class NamespaceStoreImpl implements NamespaceStore {
    private static final Logger logger = LoggerFactory.getLogger(NamespaceStoreImpl.class);

    private MongoClient mongoClient;

    public NamespaceStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(Namespace.class).build();
        CodecRegistry pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
    }

    @Override
    public List<Namespace> load() {
        logger.info("load : Namespaces addded to List");
        List<Namespace> list = new ArrayList<>();
        for (String namespace : mongoClient.listDatabaseNames()) {
            Namespace name = new Namespace();
            name.setName(namespace);
            list.add(name);
        }
        logger.trace("load : Namespaces returned as List");
        return list;
    }

    @Override
    public void store(Namespace namespace) {
        logger.trace("store : Creating Namespace as {}", namespace);
        MongoDatabase database = mongoClient.getDatabase(namespace.getName());
        logger.info("Namespaces Stored");
    }

    @Override
    public Namespace load(NamespaceId namespaceId) {
        logger.trace("load : Getting Namespace as NamespaceID {}", namespaceId);
        Namespace item = new Namespace();
        MongoDatabase database = mongoClient.getDatabase(namespaceId.getName());
        item.setName(database.getName());
        logger.info("load : Returned Namespace");
        return item;
    }

    @Override
    public void update(Namespace namespace) {
        logger.trace("update : Updating Namespace in Namespace {}", namespace);
        store(namespace);
        logger.info("update : Namespace updated");
    }

    @Override
    public void delete(NamespaceId namespaceId) {
        logger.trace("delete : NamespaceID {} detected", namespaceId);
        MongoDatabase database = mongoClient.getDatabase(namespaceId.getName());
        database.drop();
        logger.info("deleted : Namespace deleted");
    }
}
