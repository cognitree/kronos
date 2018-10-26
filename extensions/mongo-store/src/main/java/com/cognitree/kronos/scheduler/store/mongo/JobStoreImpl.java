package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOptions;
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

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class JobStoreImpl implements JobStore {

    private static final Logger logger = LoggerFactory.getLogger(Job.class);
    private static final String COLLECTION_NAME = "jobs";
    private final MongoClient mongoClient;
    private final CodecRegistry pojoCodecRegistry;

    JobStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(Job.class).build();
        pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
        BSON.addEncodingHook(Job.Status.class, new EnumTransformer());
    }

    private MongoCollection<?> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
    }

    @Override
    public List<Job> load(String namespace) {
        logger.trace("load : Job loaded to List for namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs = (FindIterable<Job>) mongoCollection.find(eq("namespace", namespace));
        for (Job job : jobs) {
            list.add(job);
        }
        logger.info("load : Job returned as List");
        return list;
    }

    @Override
    public List<Job> load(String namespace, long createdAfter, long createdBefore) {
        logger.trace("load : Job loaded to List for namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        BasicDBObject query = new BasicDBObject((Map) and(gte("createdAt", createdAfter),
                lt("createdAt", createdBefore), eq("namespace", namespace)));
        FindIterable<Job> jobs = (FindIterable<Job>) mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.info("load : Job returned as List");
        return list;
    }

    @Override
    public List<Job> loadByWorkflowName(String namespace, String workflowName, long createdAfter, long createdBefore) {
        logger.trace("loadByWorkflowName : job loaded with workflow {} for namespace {}", workflowName, namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        BasicDBObject query = new BasicDBObject("namespace", namespace).append("workflow", workflowName);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs = (FindIterable<Job>) mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.info("loadByWorkflowName : Job returned as List");
        return list;
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerName(String namespace, String workflowName, String triggerName, long createdAfter, long createdBefore) {
        logger.trace("loadByWorkflowNameAndTriggerName : job loaded with workflow {} for namespace {}", workflowName, namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        BasicDBObject query = new BasicDBObject("namespace", namespace).append("workflow", workflowName)
                .append("trigger", triggerName);
        FindIterable<Job> jobs = (FindIterable<Job>) mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.info("loadByWorkflowNameAndTriggerName : Job returned as List");
        return list;
    }

    @Override
    public List<Job> loadByStatus(String namespace, List<Job.Status> statuses, long createdAfter, long createdBefore) {
        logger.trace("loadByStatus : jobs loaded for namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs;
        BasicDBObject query = new BasicDBObject("namespace", namespace).append("$in", statuses);
        jobs = (FindIterable<Job>) mongoCollection
                .find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.info("loadByStatus : Job returned as List");
        return list;
    }

    @Override
    public List<Job> loadByWorkflowNameAndStatus(String namespace, String workflowName, List<Job.Status> statuses, long createdAfter, long createdBefore) {
        logger.trace("loadByWorkflowNameAndStatus : jobs loaded with namespace {} for workflow {}", namespace, workflowName);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName)
                .append("$in", statuses);
        jobs = (FindIterable<Job>) mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.info("loadByWorkflowNameAndStatus : Job returned as List");
        return list;
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerNameAndStatus(String namespace, String workflowName, String triggerName, List<Job.Status> statuses, long createdAfter, long createdBefore) {
        logger.trace("loadByWorkflowNameAndTriggerNameAndStatus : jobs loaded with namespace {} for workflow {}", namespace, workflowName);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName)
                .append("trigger", triggerName)
                .append("$in", statuses);
        jobs = (FindIterable<Job>) mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.info("loadByWorkflowAndTriggerNameNameAndStatus : Job returned as List");
        return list;
    }

    @Override
    public Map<Job.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) {
        logger.trace("countByStatus : Counted Jobs for namespace {}", namespace);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        Map<Job.Status, Integer> map = new TreeMap<>();
        BasicDBObject query = new BasicDBObject("$match", new BasicDBObject("namespace", namespace)
                .append("$group", new BasicDBObject("_id", "status")
                        .append("count", new BasicDBObject("$sum", 1))));
        FindIterable<Document> jobs = (FindIterable<Document>) mongoCollection.find(query);
        for (Document job : jobs) {
            map.put((Job.Status) job.get("_id"), job.getInteger("count"));
        }
        logger.info("loadByStatus : Jobs returned as Map");
        return map;
    }

    @Override
    public Map<Job.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName, long createdAfter, long createdBefore) {
        logger.trace("countByStatusForWorkflowName : Counted Jobs for namespace {} with workflow {}", namespace, workflowName);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        Map<Job.Status, Integer> map = new TreeMap<>();
        BasicDBObject query = new BasicDBObject("$match", new BasicDBObject("namespace", namespace)
                .append("$match", new BasicDBObject("name", workflowName))
                .append("$group", new BasicDBObject("_id", "status")
                        .append("count", new BasicDBObject("$sum", 1))));
        FindIterable<Document> jobs = (FindIterable<Document>) mongoCollection
                .find(query);
        for (Document job : jobs) {
            map.put((Job.Status) job.get("_id"), job.getInteger("count"));
        }
        logger.info("loadByStatusForWorkflowName : Jobs returned as Map");
        return map;
    }

    @Override
    public void deleteByWorkflowName(String namespace, String workflowName) {
        logger.trace("deleteByWorkflowName : deleted Jobs for namespace {} with workflow {}", namespace, workflowName);
        MongoCollection<?> mongoCollection = getMongoCollection(namespace);
        mongoCollection.deleteOne(eq("id", workflowName),
                (DeleteOptions) combine(eq("namespace", namespace)));
    }

    @Override
    public void store(Job job) {
        logger.trace("store : Stored Job for namespace {} with workflow {}", job.getNamespace(), job.getWorkflow());
        MongoDatabase database = mongoClient.getDatabase(job.getNamespace());
        MongoCollection<Job> jobCollection = database.getCollection(COLLECTION_NAME, Job.class);
        jobCollection.insertOne(job);
    }

    @Override
    public Job load(JobId jobId) {
        logger.trace("load : Loaded Job for namespace {} with workflow {}", jobId.getNamespace(), jobId.getWorkflow());
        MongoCollection<?> mongoCollection = getMongoCollection(jobId.getNamespace());
        return (Job) mongoCollection.find(eq("id", jobId.getId())).first();
    }

    @Override
    public void update(Job job) {
        logger.trace("update : Updated Job for namespace {} with workflow {}", job.getNamespace(), job.getWorkflow());
        MongoCollection<?> mongoCollection = getMongoCollection(job.getNamespace());
        mongoCollection.findOneAndUpdate(
                eq("id", job.getId()), (set("workflow", job.getWorkflow())));
    }

    @Override
    public void delete(JobId jobId) {
        logger.trace("delete : deleted Job for namespace {} with workflow {}", jobId.getNamespace(), jobId.getWorkflow());
        MongoCollection<?> mongoCollection = getMongoCollection(jobId.getNamespace());
        mongoCollection.deleteOne(eq("id", jobId.getId()),
                (DeleteOptions) combine(eq("namespace", jobId.getNamespace())));
    }
}
