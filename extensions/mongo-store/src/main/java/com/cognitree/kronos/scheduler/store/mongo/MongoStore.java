package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.store.StoreException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoStore.class);

    private final MongoClient mongoClient;

    MongoStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public <T> void insertOne(String database, String collection, T document, Class<T> documentType) throws StoreException {
        try {
            getCollection(database, collection, documentType).insertOne(document);
        } catch (Exception e) {
            logger.error("Error storing document {} into database {}, collection {}",
                    document, database, collection, e);
            throw new StoreException(e);
        }
    }

    public <T> T findOne(String database, String collection, Bson filter, Class<T> documentType) throws StoreException {
        try {
            return getCollection(database, collection, documentType).find(filter).first();
        } catch (Exception e) {
            logger.error("Error finding document with filter {} in database {}, collection {}",
                    filter, database, collection, e);
            throw new StoreException(e);
        }
    }

    public <T> ArrayList<T> findMany(String database, String collection, Bson filter, Class<T> documentType) throws StoreException {
        try {
            return getCollection(database, collection, documentType).find(filter).into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error finding document with filter {} in database {}, collection {}",
                    filter, database, collection, e);
            throw new StoreException(e);
        }
    }

    public <T> ArrayList<T> findAll(String database, String collection, Class<T> documentType) throws StoreException {
        try {
            return getCollection(database, collection, documentType).find().into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error finding all document in database {}, collection {}", database, collection, e);
            throw new StoreException(e);
        }
    }


    public <T> void findOneAndUpdate(String database, String collection, Bson filter, Bson update, Class<T> documentType) throws StoreException {
        try {
            getCollection(database, collection, documentType).findOneAndUpdate(filter, update);
        } catch (Exception e) {
            logger.error("Error updating document with filter {}, update {} in database {}, collection {}",
                    filter, update, database, collection, e);
            throw new StoreException(e);
        }
    }

    public <T> void deleteOne(String database, String collection, Bson filter, Class<T> documentType) throws StoreException {
        try {
            getCollection(database, collection, documentType).deleteOne(filter);
        } catch (Exception e) {
            logger.error("Error deleting document {} from database {}, collection {}",
                    filter, database, collection, e);
            throw new StoreException(e);
        }
    }

    public <T> ArrayList<T> aggregate(String database, String collection, List<Bson> pipelines, Class<T> documentType) throws StoreException {
        try {
            return getCollection(database, collection, documentType).aggregate(pipelines).into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error aggregating pipelines {} in database {}, collection {}", pipelines,
                    database, collection, e);
            throw new StoreException(e);
        }
    }

    public void dropDatabase(String database) throws StoreException {
        try {
            mongoClient.getDatabase(database).drop();
        } catch (Exception e) {
            logger.error("Error dropping database {}", database, e);
            throw new StoreException(e);
        }
    }

    public <T> MongoCollection<T> getCollection(String database, String collection, Class<T> documentType) {
        return mongoClient.getDatabase(database).getCollection(collection, documentType);
    }
}
