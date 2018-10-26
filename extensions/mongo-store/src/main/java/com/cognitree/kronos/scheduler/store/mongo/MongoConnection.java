package com.cognitree.kronos.scheduler.store.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoConnection {

    private static final Logger logger = LoggerFactory.getLogger(MongoConnection.class);

    private static final String host = "localhost";
    private MongoClient mongoClient = MongoClients.create();
    MongoDatabase database;
    MongoCollection<?> mongoCollection;

    void getConnection(String dbNAme, String collectionName, Class className) {
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(className).build();
        CodecRegistry pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
        mongoClient = MongoClients.create(
                MongoClientSettings.builder().applyToClusterSettings
                        (builder -> builder.hosts(Arrays.asList(new ServerAddress(host)))).build());
        database = mongoClient.getDatabase(dbNAme);
        mongoCollection = database.getCollection(collectionName, className);
        MongoClientSettings settings = MongoClientSettings.builder()
                .codecRegistry(pojoCodecRegistry)
                .build();
        mongoClient = MongoClients.create(settings);
        database = database.withCodecRegistry(pojoCodecRegistry);
        mongoCollection = mongoCollection.withCodecRegistry(pojoCodecRegistry);
    }
}
