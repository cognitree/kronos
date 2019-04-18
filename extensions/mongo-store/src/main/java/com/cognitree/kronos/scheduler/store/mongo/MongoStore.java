package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.store.StoreException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoStore<T> {

    private static final Logger logger = LoggerFactory.getLogger(MongoStore.class);

    private final MongoClient mongoClient;
    private final Class<T> documentType;

    MongoStore(MongoClient mongoClient, Class<T> documentType) {
        this.mongoClient = mongoClient;
        this.documentType = documentType;
    }

    public void insertOne(String database, String collection, T document) throws StoreException {
        try {
            getCollection(database, collection).insertOne(document);
        } catch (Exception e) {
            logger.error("Error storing document {} into database {}, collection {}",
                    document, database, collection, e);
            throw new StoreException(e);
        }
    }

    public T findOne(String database, String collection, Bson filter) throws StoreException {
        try {
            return getCollection(database, collection).find(filter).first();
        } catch (Exception e) {
            logger.error("Error finding document with filter {} in database {}, collection {}",
                    filter, database, collection, e);
            throw new StoreException(e);
        }
    }

    public ArrayList<T> findMany(String database, String collection, Bson filter) throws StoreException {
        try {
            return getCollection(database, collection).find(filter).into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error finding document with filter {} in database {}, collection {}",
                    filter, database, collection, e);
            throw new StoreException(e);
        }
    }

    public ArrayList<T> findAll(String database, String collection) throws StoreException {
        try {
            return getCollection(database, collection).find().into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error finding all document in database {}, collection {}", database, collection, e);
            throw new StoreException(e);
        }
    }


    public void findOneAndUpdate(String database, String collection, Bson filter, Bson update) throws StoreException {
        try {
            getCollection(database, collection).findOneAndUpdate(filter, update);
        } catch (Exception e) {
            logger.error("Error updating document with filter {}, update {} in database {}, collection {}",
                    filter, update, database, collection, e);
            throw new StoreException(e);
        }
    }

    public void deleteOne(String database, String collection, Bson filter) throws StoreException {
        try {
            getCollection(database, collection).deleteOne(filter);
        } catch (Exception e) {
            logger.error("Error deleting document {} from database {}, collection {}",
                    filter, database, collection, e);
            throw new StoreException(e);
        }
    }

    public ArrayList<Document> aggregate(String database, String collection, List<Bson> pipelines) throws StoreException {
        try {
            MongoCollection<Document> mongoCollection =
                    mongoClient.getDatabase(database).getCollection(collection, Document.class);
            return mongoCollection.aggregate(pipelines).into(new ArrayList<>());
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

    public MongoCollection<T> getCollection(String database, String collection) {
        return mongoClient.getDatabase(database).getCollection(collection, documentType);
    }
}
