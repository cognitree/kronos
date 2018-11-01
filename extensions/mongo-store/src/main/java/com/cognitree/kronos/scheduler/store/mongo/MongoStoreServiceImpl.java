package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.scheduler.store.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.novemberain.quartz.mongodb.MongoDBJobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * A standard MongoDB connection based implementation of {@link StoreService}.
 */
public class MongoStoreServiceImpl extends StoreService {

    private static final Logger logger = LoggerFactory.getLogger(MongoStoreServiceImpl.class);

    protected static final String HOST = "host";
    private static final String PORT_NUMBER = "portNumber";

    private String host;
    private int portNumber;

    private MongoClient mongoClient;

    private NamespaceStore namespaceStore;
    private WorkflowStore workflowStore;
    private WorkflowTriggerStore workflowTriggerStore;
    private JobStore jobStore;
    private TaskStore taskStore;
    private org.quartz.spi.JobStore quartzJobStore;

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
        quartzJobStore = getQuartJobStore(mongoClient);
    }

    private org.quartz.spi.JobStore getQuartJobStore(MongoClient quartzMongoClient) {
        MongoDatabase database = quartzMongoClient.getDatabase(config.get("databaseName").asText());
        return new MongoDBJobStore(database);
    }

    private void initMongoClient() {
        logger.info("initMongoClient : Starting MongoDB store service");
        mongoClient = MongoClients.create(
                MongoClientSettings.builder().applyToClusterSettings
                        (builder -> builder.hosts(Collections.singletonList(new ServerAddress(host, portNumber)))).build());
    }

    @Override
    public void init() {
        logger.info("init : Initializing MongoDB store service");
        parseConfig();
    }

    @Override
    public void start() {
        logger.info("start : Starting MongoDB store service");
        initMongoClient();
        createStore();
        ServiceProvider.registerService(this);
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

    private void parseConfig() {
        host = config.get(HOST).asText();
        portNumber = Integer.parseInt(config.get(PORT_NUMBER).asText());
    }
}
