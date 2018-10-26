package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.store.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class MongoStoreServiceImpl extends StoreService {
    private static final Logger logger = LoggerFactory.getLogger(MongoStoreServiceImpl.class);
    protected MongoClient mongoClient;
    private NamespaceStore namespaceStore;
    private WorkflowStore workflowStore;
    private WorkflowTriggerStore workflowTriggerStore;
    private JobStore jobStore;
    private TaskStore taskStore;
    private org.quartz.spi.JobStore quartzJobStore;
    private String dbNAme;
    private String host;
    private MongoDatabase database;
    private CodecRegistry pojoCodecRegistry;

    public MongoStoreServiceImpl(ObjectNode config) {
        super(config);
    }

    @Override
    public NamespaceStore getNamespaceStore() {
        return namespaceStore;
    }

    @Override
    public WorkflowStore getWorkflowStore() {
        return workflowStore;
    }

    @Override
    public WorkflowTriggerStore getWorkflowTriggerStore() {
        return workflowTriggerStore;
    }

    @Override
    public JobStore getJobStore() {
        return jobStore;
    }

    @Override
    public TaskStore getTaskStore() {
        return taskStore;
    }

    @Override
    public org.quartz.spi.JobStore getQuartzJobStore() {
        return quartzJobStore;
    }

    private void createStore() {
        namespaceStore = new NamespaceStoreImpl(mongoClient);
        workflowStore = new WorkflowStoreImpl(mongoClient);
        workflowTriggerStore = new WorkflowTriggerStoreImpl(mongoClient);
        jobStore = new JobStoreImpl(mongoClient);
        taskStore = new TaskStoreImpl(mongoClient);
    }

    @Override
    public void init() {
        logger.info("init : Initializing MongoDB store service");
        String host = "localhost";
        int portnumber = 27017;
        createStore();
    }

    @Override
    public void start() {
        logger.info("start : Starting MongoDB store service");
        mongoClient = MongoClients.create(
                MongoClientSettings.builder().applyToClusterSettings
                        (builder -> builder.hosts(Collections.singletonList(new ServerAddress(host)))).build());
        database = mongoClient.getDatabase(dbNAme);
        MongoClientSettings settings = MongoClientSettings.builder()
                .codecRegistry(pojoCodecRegistry)
                .build();
        mongoClient = MongoClients.create(settings);
    }

    @Override
    public void stop() {
        logger.info("stop : Stopping MongoDB store service");
        try {
            mongoClient.close();
        } catch (Exception e) {
            logger.error("stop : Error closing data source", e);
        }
    }
}
