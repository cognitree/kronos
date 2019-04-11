package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.model.Job;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BSON;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class Test {

    private static final Logger logger = LoggerFactory.getLogger(Test.class);
    private static final String host = "localhost";
    private static Test ourInstance = new Test();
    MongoDatabase database;
    MongoCollection<?> mongoCollection;
    private MongoClient mongoClient;

    public static Test getInstance() {
        return ourInstance;
    }

    MongoCollection<?> getConnection(String dbNAme, String collectionName, Class className) {
        logger.info("getConnection : Initializing connection for {}",dbNAme );
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(className).build();
        CodecRegistry pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
        BSON.addEncodingHook(Task.Status.class, new EnumTransformer());
        BSON.addEncodingHook(Job.Status.class, new EnumTransformer());
        mongoClient = MongoClients.create(
                MongoClientSettings.builder().applyToClusterSettings
                        (builder -> builder.hosts(Collections.singletonList(new ServerAddress(host)))).build());
        database = mongoClient.getDatabase(dbNAme);
        mongoCollection = database.getCollection(collectionName, className);
        MongoClientSettings settings = MongoClientSettings.builder()
                .codecRegistry(pojoCodecRegistry)
                .build();
        mongoClient = MongoClients.create(settings);
        database = database.withCodecRegistry(pojoCodecRegistry);
        mongoCollection = mongoCollection.withCodecRegistry(pojoCodecRegistry);
        logger.trace("getConnection : Returned the mongoCollection for {} database {}",collectionName,dbNAme);
        return mongoCollection;
    }
}
