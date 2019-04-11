package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.BSON;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * A standard MongoDB based implementation of {@link TaskStore}.
 */
public class TaskStoreImpl implements TaskStore {

    private static final Logger logger = LoggerFactory.getLogger(TaskStoreImpl.class);

    private static final String COLLECTION_NAME = "tasks";

    private final CodecRegistry pojoCodecRegistry;
    private final MongoClient mongoClient;

    TaskStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        ClassModelBuilder<Task> taskClassModelBuilder =
                ClassModel.builder(Task.class);
        ClassModel<Task> taskClassModel = taskClassModelBuilder.build();
        PojoCodecProvider codecProvider = PojoCodecProvider.builder()
                .automatic(true)
                .register(taskClassModel)
                .build();
        pojoCodecRegistry = fromRegistries(
                com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
        BSON.addEncodingHook(Task.Status.class, new EnumTransformer());
    }

    @Override
    public List<Task> load(String namespace) {
        logger.debug("load : Task loaded to List for namespace {}", namespace);
        List<Task> list = new ArrayList<>();
        MongoCollection<Task> mongoCollection = getMongoCollection(namespace);
        FindIterable<Task> tasks = mongoCollection.find();
        for (Task task : tasks) {
            list.add(task);
        }
        logger.info("load : returned {} tasks for namespace {}", list.size(), namespace);
        return list;
    }

    @Override
    public List<Task> loadByJobIdAndWorkflowName(String namespace, String jobId, String workflowName) {
        logger.debug("loadByJobIdAndWorkflowName : Task loaded to List for namespace {}", namespace);
        MongoCollection<Task> mongoCollection = getMongoCollection(namespace);
        List<Task> list = new ArrayList<>();
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("job", jobId)
                .append("workflow", new BasicDBObject("$eq", workflowName));
        FindIterable<Task> tasks = mongoCollection.find(query);
        for (Task task : tasks) {
            list.add(task);
        }
        logger.info("loadByJobIdAndWorkflowName : returned {} tasks under workflow name {} for namespace {}",
                list.size(), workflowName, namespace);
        return list;
    }

    @Override
    public List<Task> loadByStatus(String namespace, List<Task.Status> statuses) {
        logger.debug("loadByStatus : Task loaded to List for namespace {}", namespace);
        MongoCollection<Task> mongoCollection = getMongoCollection(namespace);
        List<Task> list = new ArrayList<>();
        FindIterable<Task> tasks;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("status", new BasicDBObject("$in", statuses));
        tasks = mongoCollection.find(query);
        for (Task task : tasks) {
            list.add(task);
        }
        logger.info("loadByStatus : returned {} tasks under workflow name {} for namespace {}",
                list.size(), namespace);
        return list;
    }

    @Override
    public Map<Task.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) {
        logger.debug("countByStatus : Counted Task for namespace {}", namespace);
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
        Map<Task.Status, Integer> map = new TreeMap<>();
        List<BasicDBObject> pipeline = new ArrayList<>();
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("namespace", namespace));
        BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$status")
                .append("total", new BasicDBObject("$sum", 1)));
        pipeline.add(match);
        pipeline.add(group);
        AggregateIterable<Document> tasks = mongoCollection.aggregate(pipeline);
        for (Document task : tasks) {
            String temp = (String) task.get("_id");
            Task.Status status = Task.Status.valueOf(temp.toUpperCase(Locale.ENGLISH));
            map.put(status, task.getInteger("total"));
        }
        logger.info("loadByStatus : Returned {} tasks for namespace {} created after {} created before {}",
                map.size(), namespace, createdAfter, createdBefore);
        return map;
    }

    @Override
    public Map<Task.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName,
                                                                  long createdAfter, long createdBefore) {
        logger.debug("countByStatusForWorkflowName : Counted Task for namespace {} by status", namespace);
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
        Map<Task.Status, Integer> map = new TreeMap<>();
        List<BasicDBObject> pipeline = new ArrayList<>();
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName));
        BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$status")
                .append("total", new BasicDBObject("$sum", 1)));
        pipeline.add(match);
        pipeline.add(group);
        AggregateIterable<Document> tasks = mongoCollection.aggregate(pipeline);
        for (Document task : tasks) {
            String temp = (String) task.get("_id");
            Task.Status status = Task.Status.valueOf(temp.toUpperCase(Locale.ENGLISH));
            map.put(status, task.getInteger("total"));
        }
        logger.info("countByStatusForWorkflowName : Returned {} tasks for namespace {}" +
                        " created after {} created before {} for workflow {}",
                map.size(), namespace, createdAfter, createdBefore, workflowName);
        return map;
    }

    @Override
    public void store(Task task) {
        logger.debug("store : Task {} for {} loaded to List with namespace {}",
                task.getName(), task.getWorkflow(), task.getNamespace());
        MongoCollection<Task> taskCollection = getMongoCollection(task.getNamespace());
        taskCollection.insertOne(task);
    }

    @Override
    public Task load(TaskId taskId) {
        logger.debug("load : Task {} with taskID  {} for {} loaded to List with namespace {}",
                taskId.getName(), taskId.getWorkflow(), taskId.getNamespace());
        MongoCollection<Task> mongoCollection = getMongoCollection(taskId.getNamespace());
        return mongoCollection.find(eq("job", taskId.getJob())).first();
    }

    @Override
    public void update(Task task) {
        logger.debug("update : Task {} with taskID  {} for {} updated to List with namespace {}",
                task.getName(), task.getWorkflow(), task.getNamespace());
        MongoCollection<Task> mongoCollection = getMongoCollection(task.getNamespace());
        mongoCollection.updateMany(
                eq("namespace", task.getNamespace()), (set("status", task.getStatus().toString())));
    }

    @Override
    public void delete(TaskId taskId) {
        logger.debug("delete : Task {} with taskID  {} for {} deleted for namespace {}",
                taskId.getName(), taskId.getWorkflow(), taskId.getNamespace());
        MongoCollection<Task> mongoCollection = getMongoCollection(taskId.getNamespace());
        mongoCollection.deleteOne(eq("job", taskId.getJob()));
    }

    private MongoCollection<Task> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME, Task.class);
    }
}
