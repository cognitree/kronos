package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOptions;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class WorkflowTriggerStoreImpl implements WorkflowTriggerStore {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowTriggerStoreImpl.class);
    private static final String COLLECTION_NAME = "workflowtriggers";
    private final CodecRegistry pojoCodecRegistry;
    private final MongoClient mongoClient;

    WorkflowTriggerStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(WorkflowTrigger.class).build();
        pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
    }

    private MongoCollection<?> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
    }

    @Override
    public List<WorkflowTrigger> load(String namespace) {
        logger.trace("load : WorkflowTrigger loaded to List with namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<WorkflowTrigger> list = new ArrayList<>();
        FindIterable<WorkflowTrigger> workflowTriggers = (FindIterable<WorkflowTrigger>) mongoCollection.find();
        for (WorkflowTrigger workflowTrigger : workflowTriggers) {
            list.add(workflowTrigger);
        }
        logger.info("load : WorkflowTrigger returned as List");
        return list;
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowName(String namespace, String workflowName) {
        logger.trace("loadByWorkflowName : WorkflowTrigger for {} loaded to List with namespace {}", workflowName, namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<WorkflowTrigger> list = new ArrayList<>();
        FindIterable<WorkflowTrigger> workflowtriggers;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName);
        workflowtriggers = (FindIterable<WorkflowTrigger>) mongoCollection.find(query);
        for (WorkflowTrigger workflowtrigger : workflowtriggers) {
            list.add(workflowtrigger);
        }
        logger.info("loadByWorkflowName : WorkflowTrigger returned as List");
        return list;
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowNameAndEnabled(String namespace, String workflowName, boolean enabled) {
        logger.trace("loadByWorkflowNameAndEnabled : WorkflowTrigger for {} loaded to List with namespace {}", workflowName, namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        ArrayList<WorkflowTrigger> list = new ArrayList<>();
        FindIterable<WorkflowTrigger> workflowtriggers;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName)
                .append("enabled", enabled);
        workflowtriggers = (FindIterable<WorkflowTrigger>) mongoCollection.find(query);
        for (WorkflowTrigger workflowtrigger : workflowtriggers) {
            list.add(workflowtrigger);
        }
        logger.info("loadByWorkflowNameAndEnabled : WorkflowTrigger returned as List");
        return list;
    }

    @Override
    public void store(WorkflowTrigger workflowTrigger) {
        logger.trace("store : WorkflowTrigger {} for {} loaded to List with namespace {}", workflowTrigger.getName(), workflowTrigger.getWorkflow(), workflowTrigger.getNamespace());
        MongoDatabase database=mongoClient.getDatabase(workflowTrigger.getNamespace());
        MongoCollection<WorkflowTrigger> workflowTriggerCollection = database.getCollection(COLLECTION_NAME, WorkflowTrigger.class);
        workflowTriggerCollection.insertOne(workflowTrigger);
        logger.info("store : WorkflowTrigger stored as List");
    }

    @Override
    public WorkflowTrigger load(WorkflowTriggerId workflowTriggerId) {
        logger.trace("load : WorkflowTrigger {} for {} loaded with namespace {}", workflowTriggerId.getName(), workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(workflowTriggerId.getNamespace());
        return (WorkflowTrigger) mongoCollection.find(eq("name", workflowTriggerId.getName())).first();
    }

    @Override
    public void update(WorkflowTrigger workflowTrigger) {
        logger.trace("update : updated WorkflowTrigger {} for {}  with namespace {}", workflowTrigger.getName(), workflowTrigger.getWorkflow(), workflowTrigger.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(workflowTrigger.getNamespace());
        mongoCollection.findOneAndUpdate(
                eq("name", workflowTrigger.getName()), (set("workflow", workflowTrigger.getWorkflow())));
    }

    @Override
    public void delete(WorkflowTriggerId workflowTriggerId) {
        logger.trace("delete : WorkflowTrigger {} for {} deleted with namespace {}", workflowTriggerId.getName(), workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(workflowTriggerId.getNamespace());
        mongoCollection.deleteOne(eq("name", workflowTriggerId.getName()),
                (DeleteOptions) combine(eq("namespace", workflowTriggerId.getNamespace())));
    }
}
