package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * A standard MongoDB based implementation of {@link NamespaceStore}.
 */
public class NamespaceStoreImpl implements NamespaceStore {

    private static final Logger logger = LoggerFactory.getLogger(NamespaceStoreImpl.class);
    private static final String COLLECTION_NAME = "config";

    private final MongoClient mongoClient;
    private final CodecRegistry pojoCodecRegistry;

    NamespaceStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        PojoCodecProvider codecProvider = PojoCodecProvider.builder()
                .automatic(true)
                .register(Namespace.class)
                .build();
        pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
    }

    @Override
    public List<Namespace> load() {
        logger.debug("load : Namespaces addded to List");
        List<Namespace> list = new ArrayList<>();
        for (String namespace : mongoClient.listDatabaseNames()) {
            Namespace name = new Namespace();
            name.setName(namespace);
            list.add(name);
        }
        logger.debug("load : Namespaces returned as List");
        return list;
    }

    @Override
    public void store(Namespace namespace) {
        logger.debug("store : Created Namespace as {}", namespace);
        MongoDatabase database = mongoClient.getDatabase(namespace.getName()).withCodecRegistry(pojoCodecRegistry);
        database.createCollection(COLLECTION_NAME);
        MongoCollection<Namespace> namespaceMongoCollection = database.getCollection(COLLECTION_NAME, Namespace.class);
        namespaceMongoCollection.insertOne(namespace);
        logger.debug("Namespace Stored as {}", namespace);
    }

    @Override
    public Namespace load(NamespaceId namespaceId) {
        logger.debug("load : Request Namespace as NamespaceID {}", namespaceId);
        boolean flag = false;
        MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
        for (String temp : dbNames)
            if (temp.equals(namespaceId.getName())) {
                flag = true;
                break;
            }
        if (flag) {
            Namespace namespace=new Namespace();
            namespace.setName(namespaceId.getName());
            namespace.setDescription(namespaceId.toString());
            return namespace;
        } else
            return null;
    }

    @Override
    public void update(Namespace namespace) {
        logger.debug("update : Updating Namespace in Namespace {}", namespace);
        store(namespace);
        logger.debug("update : Namespace updated as {}", namespace);
    }

    @Override
    public void delete(NamespaceId namespaceId) {
        logger.debug("delete : NamespaceID {} detected", namespaceId);
        MongoDatabase database = mongoClient.getDatabase(namespaceId.getName());
        database.drop();
        logger.debug("deleted : Namespace deleted for {}", namespaceId.getName());
    }
}
