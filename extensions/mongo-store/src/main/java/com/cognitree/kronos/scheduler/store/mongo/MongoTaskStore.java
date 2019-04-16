package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * A standard MongoDB based implementation of {@link TaskStore}.
 */
public class MongoTaskStore implements TaskStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoTaskStore.class);

    private static final String COLLECTION_NAME = "tasks";

    private final MongoClient mongoClient;

    MongoTaskStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public void store(Task task) throws StoreException {
        logger.debug("Received request to store task {}", task);
        try {
            MongoCollection<Task> taskCollection = getMongoCollection(task.getNamespace());
            taskCollection.insertOne(task);
        } catch (Exception e) {
            logger.error("Error storing task {}", task, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Task> load(String namespace) throws StoreException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        try {
            MongoCollection<Task> mongoCollection = getMongoCollection(namespace);
            return mongoCollection.find().into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error loading all tasks under namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Task load(TaskId taskId) throws StoreException {
        logger.debug("Received request to load task with id {}", taskId);
        try {
            MongoCollection<Task> mongoCollection = getMongoCollection(taskId.getNamespace());
            return mongoCollection.find(
                    and(
                            eq("name", taskId.getName()),
                            eq("job", taskId.getJob()),
                            eq("workflow", taskId.getWorkflow()),
                            eq("namespace", taskId.getNamespace())))
                    .first();
        } catch (Exception e) {
            logger.error("Error loading task with id {}", taskId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Task> loadByJobIdAndWorkflowName(String namespace, String jobId, String workflowName) throws StoreException {
        logger.debug("Received request to get all tasks with job id {}, workflow name {}, namespace {}",
                jobId, workflowName, namespace);
        try {
            MongoCollection<Task> mongoCollection = getMongoCollection(namespace);
            return mongoCollection.find(
                    and(
                            eq("job", jobId),
                            eq("workflow", workflowName),
                            eq("namespace", namespace)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching tasks with job id {}, workflow name {}, namespace {}",
                    jobId, workflowName, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Task> loadByStatus(String namespace, List<Task.Status> statuses) throws StoreException {
        logger.debug("Received request to get all tasks with status in {} under namespace {}", statuses, namespace);
        try {
            MongoCollection<Task> mongoCollection = getMongoCollection(namespace);
            return mongoCollection.find(
                    and(
                            in("status", statuses),
                            eq("namespace", namespace)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching task with status in {} under namespace {}", statuses, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Map<Task.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count all tasks by status under namespace {}," +
                "created after {}, created before {}", namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(namespace)
                    .getCollection(COLLECTION_NAME);
            List<Bson> pipelines = new ArrayList<>();
            pipelines.add(Aggregates.match(
                    and(
                            eq("namespace", namespace),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter))));

            pipelines.add(Aggregates.group("$status", Accumulators.sum("count", 1)));

            AggregateIterable<Document> tasks = mongoCollection.aggregate(pipelines);
            Map<Task.Status, Integer> statusMap = new HashMap<>();
            for (Document task : tasks) {
                Task.Status status = Task.Status.valueOf(task.get("_id").toString());
                statusMap.put(status, task.getInteger("count"));
            }
            return statusMap;
        } catch (Exception e) {
            logger.error("Error loading all tasks under namespace {}, created after {}, created before {}",
                    namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Map<Task.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName,
                                                                  long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count tasks by status having workflow name {} under namespace {}," +
                "created after {}, created before {}", workflowName, namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(namespace)
                    .getCollection(COLLECTION_NAME);
            List<Bson> pipelines = new ArrayList<>();
            pipelines.add(Aggregates.match(
                    and(
                            eq("namespace", namespace),
                            eq("workflow", workflowName),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter))));
            pipelines.add(Aggregates.group("$status", Accumulators.sum("count", 1)));
            AggregateIterable<Document> tasks = mongoCollection.aggregate(pipelines);
            Map<Task.Status, Integer> statusMap = new HashMap<>();
            for (Document task : tasks) {
                Task.Status status = Task.Status.valueOf(task.get("_id").toString());
                statusMap.put(status, task.getInteger("count"));
            }
            return statusMap;
        } catch (Exception e) {
            logger.error("Error counting tasks by status having workflow name {} under namespace {}," +
                    "created after {}, created before {}", workflowName, namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void update(Task task) throws StoreException {
        logger.debug("Received request to update task to {}", task);
        try {
            MongoCollection<Task> mongoCollection = getMongoCollection(task.getNamespace());
            mongoCollection.updateMany(
                    and(
                            eq("name", task.getName()),
                            eq("job", task.getJob()),
                            eq("workflow", task.getWorkflow())),
                    combine(
                            set("status", task.getStatus().toString()),
                            set("statusMessage", task.getStatusMessage()),
                            set("submittedAt", task.getSubmittedAt()),
                            set("completedAt", task.getCompletedAt()),
                            set("context", task.getContext())));
        } catch (Exception e) {
            logger.error("Error updating task with to {}", task, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(TaskId taskId) throws StoreException {
        logger.debug("Received request to delete task with id {}", taskId);
        try {
            MongoCollection<Task> mongoCollection = getMongoCollection(taskId.getNamespace());
            mongoCollection.deleteOne(eq("job", taskId.getJob()));
        } catch (Exception e) {
            logger.error("Error deleting task with id {}", taskId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private MongoCollection<Task> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace).getCollection(COLLECTION_NAME, Task.class);
    }
}
