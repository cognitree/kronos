package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

/**
 * A standard MongoDB based implementation of {@link NamespaceStore}.
 */
public class MongoNamespaceStore implements NamespaceStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoNamespaceStore.class);

    private static final String DATABASE_NAME = "namespace";
    private static final String COLLECTION_NAME = "config";

    private final MongoClient mongoClient;

    MongoNamespaceStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public void store(Namespace namespace) throws StoreException {
        logger.debug("Received request to store namespace {}", namespace);
        try {
            MongoCollection<Namespace> namespaceCollection = getNamespaceCollection();
            namespaceCollection.insertOne(namespace);
        } catch (Exception e) {
            logger.error("Error storing namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Namespace> load() throws StoreException {
        logger.debug("Received request to get all namespaces");
        try {
            MongoCollection<Namespace> namespaceCollection = getNamespaceCollection();
            return namespaceCollection.find().into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching all namespaces", e);
            throw new StoreException(e.getMessage(), e.getCause());
        }

    }

    @Override
    public Namespace load(NamespaceId namespaceId) throws StoreException {
        logger.debug("Received request to load namespace with id {}", namespaceId);

        try {
            MongoCollection<Namespace> namespaceCollection = getNamespaceCollection();
            return namespaceCollection.find(eq("name", namespaceId.getName())).first();
        } catch (Exception e) {
            logger.error("Error fetching namespace with id {}", namespaceId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void update(Namespace namespace) throws StoreException {
        logger.debug("Received request to update namespace to {}", namespace);
        try {
            MongoCollection<Namespace> namespaceCollection = getNamespaceCollection();
            namespaceCollection.findOneAndUpdate(
                    eq("name", namespace.getName()),
                    set("description", namespace.getDescription()));
        } catch (Exception e) {
            logger.error("Error updating namespace to {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(NamespaceId namespaceId) throws StoreException {
        logger.debug("Received request to delete job with id {}", namespaceId);
        try {
            MongoDatabase namespaceDatabase = mongoClient.getDatabase(namespaceId.getName());
            namespaceDatabase.drop();
            getNamespaceCollection().deleteOne(eq("name", namespaceId.getName()));
        } catch (Exception e) {
            logger.error("Error deleting namespace with id {}", namespaceId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private MongoCollection<Namespace> getNamespaceCollection() {
        return mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME, Namespace.class);
    }
}
