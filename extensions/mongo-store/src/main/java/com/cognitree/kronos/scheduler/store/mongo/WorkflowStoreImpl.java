package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class WorkflowStoreImpl implements WorkflowStore {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowStoreImpl.class);
    private static final String COLLECTION_NAME = "workflows";
    private final MongoClient mongoClient;
    private final CodecRegistry pojoCodecRegistry;

    WorkflowStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(Workflow.class).build();
        pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
    }

    private MongoCollection<?> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
    }

    @Override
    public List<Workflow> load(String namespace) throws RuntimeException {
        logger.trace("load : Workflow loaded to List with namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Workflow> list = new ArrayList<>();
        FindIterable<Workflow> workflows = (FindIterable<Workflow>) mongoCollection.find();
        for (Workflow workflow : workflows) {
            list.add(workflow);
        }
        logger.info("load : Workflow returned as List");
        return list;
    }

    @Override
    public void store(Workflow workflow) {
        logger.trace("load : Workflow {} addded to List in namespace {}", workflow.getName(), workflow.getNamespace());
        MongoDatabase database = mongoClient.getDatabase(workflow.getNamespace());
        MongoCollection<Workflow> workflowCollection = database.getCollection("workflows", Workflow.class);
        workflowCollection.insertOne(workflow);
    }

    @Override
    public Workflow load(WorkflowId workflowId) {
        logger.trace("load : Workflow {} loaded with namespace {}", workflowId.getName(), workflowId.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(workflowId.getNamespace());
        return (Workflow) mongoCollection.find(eq("name", workflowId.getName())).first();
    }

    @Override
    public void update(Workflow workflow) {
        logger.trace("load : Workflow {} updated in namespace {}", workflow.getName(), workflow.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(workflow.getNamespace());
        mongoCollection.findOneAndUpdate(
                eq("name", workflow.getName()), (set("description", workflow.getDescription())));
    }

    @Override
    public void delete(WorkflowId workflowId) {
        logger.trace("load : Workflow {} deleted in namespace {}", workflowId.getName(), workflowId.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(workflowId.getNamespace());
        mongoCollection.deleteOne(eq("name", workflowId.getName()));
    }
}
