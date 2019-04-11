package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOptions;
import org.bson.BSON;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * A standard MongoDB based implementation of {@link JobStore}.
 */
public class JobStoreImpl implements JobStore {

    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    private static final String COLLECTION_NAME = "jobs";

    private final MongoClient mongoClient;
    private final CodecRegistry pojoCodecRegistry;

    JobStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        ClassModelBuilder<Job> jobClassModelBuilder =
                ClassModel.builder(Job.class);
        ClassModel<Job> jobClassModel = jobClassModelBuilder.build();
        PojoCodecProvider codecProvider = PojoCodecProvider.builder()
                .automatic(true)
                .register(jobClassModel)
                .build();
        pojoCodecRegistry = fromRegistries(
                com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
        BSON.addEncodingHook(Job.Status.class, new EnumTransformer());
    }

    @Override
    public List<Job> load(String namespace) {
        logger.debug("load : List jobs for namespace {}", namespace);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs = mongoCollection.find(eq("namespace", namespace));
        for (Job job : jobs) {
            list.add(job);
        }
        logger.debug("load : returned {} jobs for namespace {}", list.size(), namespace);
        return list;
    }

    @Override
    public List<Job> load(String namespace, long createdAfter, long createdBefore) {
        logger.debug("load : Job loaded to List for namespace {}", namespace);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        BasicDBObject query = new BasicDBObject("namespace", namespace);
        FindIterable<Job> jobs = mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.debug("load : returned {} jobs for namespace {} created after {} and created before {}",
                list.size(), namespace, createdAfter, createdBefore);
        return list;
    }

    @Override
    public List<Job> loadByWorkflowName(String namespace, String workflowName, long createdAfter, long createdBefore) {
        logger.debug("loadByWorkflowName : job loaded with workflow {} for namespace {}", workflowName, namespace);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        BasicDBObject query = new BasicDBObject("namespace", namespace).append("workflow", workflowName);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs = mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.debug("loadByWorkflowName : returned {} jobs for namespace {}" +
                        " created after {} and created before {} for workflow {}",
                list.size(), namespace, createdAfter, createdBefore, workflowName);
        return list;
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerName(String namespace, String workflowName,
                                                      String triggerName, long createdAfter, long createdBefore) {
        logger.debug("loadByWorkflowNameAndTriggerName : job loaded with workflow {} for namespace {}",
                workflowName, namespace);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName)
                .append("trigger", triggerName);
        FindIterable<Job> jobs = mongoCollection.find(and(query));
        for (Job job : jobs) {
            list.add(job);
        }
        logger.debug("loadByWorkflowName : returned {} jobs for namespace {}" +
                        " created after {} and created before {} for workflow {} with trigger {}",
                list.size(), namespace, createdAfter, createdBefore, workflowName, triggerName);
        return list;
    }

    @Override
    public List<Job> loadByStatus(String namespace, List<Job.Status> statuses,
                                  long createdAfter, long createdBefore) {
        logger.debug("loadByStatus : jobs loaded for namespace {}", namespace);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("status", new BasicDBObject("$in", statuses));
        jobs = mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.debug("loadByStatus : returned {} jobs for namespace {}", list.size(), namespace);
        return list;
    }

    @Override
    public List<Job> loadByWorkflowNameAndStatus(String namespace, String workflowName,
                                                 List<Job.Status> statuses, long createdAfter, long createdBefore) {
        logger.debug("loadByWorkflowNameAndStatus : jobs loaded with namespace {} for workflow {}",
                namespace, workflowName);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName)
                .append("status", new BasicDBObject("$in", statuses));
        jobs = mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.debug("loadByWorkflowNameAndStatus : returned {} jobs for namespace {}", list.size(), namespace);
        return list;
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerNameAndStatus(String namespace, String workflowName,
                                                               String triggerName, List<Job.Status> statuses,
                                                               long createdAfter, long createdBefore) {
        logger.debug("loadByWorkflowNameAndTriggerNameAndStatus : jobs loaded with namespace {} for workflow {}",
                namespace, workflowName);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        List<Job> list = new ArrayList<>();
        FindIterable<Job> jobs;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName)
                .append("trigger", triggerName)
                .append("status", new BasicDBObject("$in", statuses));
        jobs = mongoCollection.find(query);
        for (Job job : jobs) {
            list.add(job);
        }
        logger.debug("loadByWorkflowAndTriggerNameNameAndStatus :" +
                " returned {} jobs for namespace {}", list.size(), namespace);
        return list;
    }

    @Override
    public Map<Job.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) {
        logger.debug("countByStatus : Counted Jobs for namespace {}", namespace);
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
        Map<Job.Status, Integer> map = new TreeMap<>();
        List<BasicDBObject> pipeline = new ArrayList<>();
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("namespace", namespace));
        BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id",
                new BasicDBObject("$status", new BasicDBObject("total", new BasicDBObject("$sum", 1)))));
        pipeline.add(match);
        pipeline.add(group);
        AggregateIterable<Document> jobs = mongoCollection.aggregate(pipeline);
        for (Document job : jobs) {
            String temp = (String) job.get("_id");
            Job.Status status = Job.Status.valueOf(temp.toUpperCase(Locale.ENGLISH));
            map.put(status, job.getInteger("total"));
        }
        logger.debug("loadByStatus : returned {} jobs for namespace {} created after {} created before {}",
                namespace, createdAfter, createdBefore);
        return map;
    }

    @Override
    public Map<Job.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName,
                                                                 long createdAfter, long createdBefore) {
        logger.debug("countByStatusForWorkflowName : Counted Jobs for namespace {} with workflow {}",
                namespace, workflowName);
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME);
        Map<Job.Status, Integer> map = new TreeMap<>();
        List<BasicDBObject> pipeline = new ArrayList<>();
        BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName));
        BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$status")
                .append("total", new BasicDBObject("$sum", 1)));
        pipeline.add(match);
        pipeline.add(group);
        AggregateIterable<Document> jobs = mongoCollection.aggregate(pipeline);
        for (Document job : jobs) {
            String temp = (String) job.get("_id");
            Job.Status status = Job.Status.valueOf(temp.toUpperCase(Locale.ENGLISH));
            map.put(status, job.getInteger("total"));
        }
        logger.debug("loadByStatusForWorkflowName : returned jobs for namespace {}" +
                        " created after {} and created before {} for workflow {} with trigger {}",
                namespace, createdAfter, createdBefore, workflowName);
        return map;
    }

    @Override
    public void deleteByWorkflowName(String namespace, String workflowName) {
        logger.debug("deleteByWorkflowName : deleted Jobs for namespace {} with workflow {}",
                namespace, workflowName);
        MongoCollection<Job> mongoCollection = getMongoCollection(namespace);
        mongoCollection.deleteOne(new BasicDBObject("workflow", workflowName));
    }

    @Override
    public void store(Job job) {
        logger.debug("store : Stored Job for namespace {} with workflow {}",
                job.getNamespace(), job.getWorkflow());
        MongoCollection<Job> jobCollection = getMongoCollection(job.getNamespace());
        jobCollection.insertOne(job);
    }

    @Override
    public Job load(JobId jobId) {
        logger.debug("load : Loaded Job for namespace {} with workflow {}",
                jobId.getNamespace(), jobId.getWorkflow());
        MongoCollection<Job> mongoCollection = getMongoCollection(jobId.getNamespace());
        return mongoCollection.find(eq("_id", jobId.getId())).first();
    }

    @Override
    public void update(Job job) {
        logger.debug("update : Updated Job for namespace {} with workflow {}",
                job.getNamespace(), job.getWorkflow());
        MongoCollection<Job> mongoCollection = getMongoCollection(job.getNamespace());
        mongoCollection.findOneAndUpdate(
                eq("_id", job.getId()), (set("status", job.getStatus().toString())));

    }

    @Override
    public void delete(JobId jobId) {
        logger.debug("delete : deleted Job for namespace {} with workflow {}",
                jobId.getNamespace(), jobId.getWorkflow());
        MongoCollection<Job> mongoCollection = getMongoCollection(jobId.getNamespace());
        mongoCollection.deleteOne(eq("_id", jobId.getId()),
                (DeleteOptions) combine(eq("namespace", jobId.getNamespace())));
    }

    private MongoCollection<Job> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME, Job.class);
    }
}
