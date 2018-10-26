package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.BSON;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class TaskStoreImpl implements TaskStore {

    private static final Logger logger = LoggerFactory.getLogger(TaskStoreImpl.class);
    private static final String COLLECTION_NAME = "tasks";
    private final CodecRegistry pojoCodecRegistry;
    private final MongoClient mongoClient;

    TaskStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(Task.class).build();
        pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
        BSON.addEncodingHook(Task.Status.class, new EnumTransformer());
    }

    @Override
    public List<Task> load(String namespace) {
        logger.trace("load : Task loaded to List for namespace {}", namespace);
        List<Task> list = new ArrayList<>();
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        FindIterable<Task> tasks = (FindIterable<Task>) mongoCollection.find();
        for (Task task : tasks) {
            list.add(task);
        }
        logger.info("load : Task returned as List");
        return list;
    }

    private MongoCollection<?> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
    }

    @Override
    public List<Task> loadByJobIdAndWorkflowName(String namespace, String jobId, String workflowName) {
        logger.trace("loadByJobIdAndWorkflowName : Task loaded to List for namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Task> list = new ArrayList<>();
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("$eq", new BasicDBObject("workflow", workflowName));
        FindIterable<Task> tasks = (FindIterable<Task>) mongoCollection.find(query);
        for (Task task : tasks) {
            list.add(task);
        }
        logger.info("loadByJobIdAndWorkflowName : Task returned as list");
        return list;
    }

    @Override
    public List<Task> loadByStatus(String namespace, List<Task.Status> statuses) {
        logger.trace("loadByStatus : Task loaded to List for namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Task> list = new ArrayList<>();
        FindIterable<Task> tasks;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("status", new BasicDBObject("$in", statuses));
        tasks = (FindIterable<Task>) mongoCollection.find(query);
        for (Task task : tasks) {
            list.add(task);
        }
        logger.info("loadByStatus : Task returned as list");
        return list;
    }

    @Override
    public Map<Task.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) {
        logger.trace("countByStatus : Counted Task for namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        Map<Task.Status, Integer> map = new TreeMap<>();
        BasicDBObject query = new BasicDBObject("$match", new BasicDBObject("namespace", namespace)
                .append("$group", new BasicDBObject("_id", "status")
                        .append("count", new BasicDBObject("$sum", 1))));
        FindIterable<Document> tasks = (FindIterable) mongoCollection.find(query);
        for (Document task : tasks) {
            map.put((Task.Status) task.get("_id"), task.getInteger("count"));
        }
        logger.info("loadByStatus : Task returned as Map");
        return map;
    }

    @Override
    public Map<Task.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName, long createdAfter, long createdBefore) {
        logger.trace("countByStatusForWorkflowName : Counted Task for namespace {} by status", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        Map<Task.Status, Integer> map = new TreeMap<>();
        BasicDBObject query = new BasicDBObject(
                "$match", new BasicDBObject("namespace", namespace)
                .append("$match", new BasicDBObject("name", workflowName))
                .append("$group", new BasicDBObject("_id", "status")
                        .append("count", new BasicDBObject("$sum", 1))));
        FindIterable<Document> tasks = (FindIterable<Document>) mongoCollection.find(query);
        for (Document task : tasks) {
            map.put((Task.Status) task.get("_id"), task.getInteger("count"));
        }
        logger.info("countByStatusForWorkflowName : Task returned as Map");
        return map;
    }

    @Override
    public void store(Task task) {
        logger.trace("store : Task {} for {} loaded to List with namespace {}", task.getName(), task.getWorkflow(), task.getNamespace());
        MongoDatabase database=mongoClient.getDatabase(task.getNamespace());
        MongoCollection<Task> taskCollection = database.getCollection(COLLECTION_NAME, Task.class);
        taskCollection.insertOne(task);
    }

    @Override
    public Task load(TaskId taskId) {
        logger.trace("load : Task {} with taskID  {} for {} loaded to List with namespace {}", taskId.getName(), taskId.getWorkflow(), taskId.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(taskId.getNamespace());
        return (Task) mongoCollection.find(eq("name", taskId.getName())).first();
    }

    @Override
    public void update(Task task) {
        logger.trace("update : Task {} with taskID  {} for {} updated to List with namespace {}", task.getName(), task.getWorkflow(), task.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(task.getNamespace());
        mongoCollection.findOneAndUpdate(
                eq("namespace", task.getNamespace()), (set("name", task.getName())));
    }

    @Override
    public void delete(TaskId taskId) {
        logger.trace("delete : Task {} with taskID  {} for {} deleted for namespace {}", taskId.getName(), taskId.getWorkflow(), taskId.getNamespace());
        MongoCollection<?> mongoCollection = getMongoCollection(taskId.getNamespace());
        mongoCollection.deleteOne((ClientSession) eq("namespace", taskId.getNamespace()), (set("name", taskId.getName())));
    }
}
