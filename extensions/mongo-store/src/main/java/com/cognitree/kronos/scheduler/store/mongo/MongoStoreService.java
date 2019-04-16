package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.CalendarIntervalSchedule;
import com.cognitree.kronos.scheduler.model.CronSchedule;
import com.cognitree.kronos.scheduler.model.DailyTimeIntervalSchedule;
import com.cognitree.kronos.scheduler.model.FixedDelaySchedule;
import com.cognitree.kronos.scheduler.model.Schedule;
import com.cognitree.kronos.scheduler.model.SimpleSchedule;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.cognitree.kronos.scheduler.store.mongo.codecs.JobStatusCodec;
import com.cognitree.kronos.scheduler.store.mongo.codecs.TaskStatusCodec;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.novemberain.quartz.mongodb.MongoDBJobStore;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * A standard MongoDB connection based implementation of {@link StoreService}.
 */
public class MongoStoreService extends StoreService {

    private static final Logger logger = LoggerFactory.getLogger(MongoStoreService.class);

    private static final String HOST = "host";
    private static final String DEFAULT_HOST = "localhost";
    private static final String PORT = "port";
    private static final int DEFAULT_PORT = 27017;
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String AUTH_DATABASE = "authDatabase";

    private String host;
    private int port;
    private MongoCredential credential;
    private MongoClient mongoClient;

    private NamespaceStore namespaceStore;
    private WorkflowStore workflowStore;
    private WorkflowTriggerStore workflowTriggerStore;
    private JobStore jobStore;
    private TaskStore taskStore;
    private org.quartz.spi.JobStore quartzJobStore;

    public MongoStoreService(ObjectNode config) {
        super(config);
    }

    @Override
    public void init() {
        logger.info("Initializing MongoDB store service");
        parseConfig();
        initMongoClient();
    }

    private void parseConfig() {
        if (config.hasNonNull(HOST)) {
            host = config.get(HOST).asText();
        } else {
            host = DEFAULT_HOST;
        }
        if (config.hasNonNull(PORT)) {
            port = Integer.parseInt(config.get(PORT).asText());
        } else {
            port = DEFAULT_PORT;
        }

        if (config.hasNonNull(USER) && config.hasNonNull(PASSWORD) && config.hasNonNull(AUTH_DATABASE)) {
            credential = MongoCredential.createCredential(config.get(USER).asText(),
                    config.get(AUTH_DATABASE).asText(), config.get(PASSWORD).asText().toCharArray());
        }
    }

    private void initMongoClient() {
        final PojoCodecProvider customCodecProvider = PojoCodecProvider.builder().automatic(true)
                .register(ClassModel.builder(Schedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(SimpleSchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(CronSchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(DailyTimeIntervalSchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(CalendarIntervalSchedule.class).enableDiscriminator(true).build())
                .build();

        final CodecRegistry codecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(customCodecProvider),
                fromCodecs(new JobStatusCodec()),
                fromCodecs(new TaskStatusCodec()),
                fromProviders(new DocumentCodecProvider()),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientSettings.Builder clusterSettings = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Collections.singletonList(new ServerAddress(host, port))))
                .codecRegistry(codecRegistry);
        if (credential != null) {
            clusterSettings.credential(credential);
        }
        mongoClient = MongoClients.create(clusterSettings.build());
    }

    @Override
    public void start() {
        logger.info("Starting MongoDB store service");
        createStore();
    }

    private void createStore() {
        namespaceStore = new MongoNamespaceStore(mongoClient);
        workflowStore = new MongoWorkflowStore(mongoClient);
        workflowTriggerStore = new MongoWorkflowTriggerStore(mongoClient);
        jobStore = new MongoJobStore(mongoClient);
        taskStore = new MongoTaskStore(mongoClient);
        quartzJobStore = getQuartJobStore(mongoClient);
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

    private org.quartz.spi.JobStore getQuartJobStore(MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(config.get("quartzDatabase").asText());
        return new MongoDBJobStore(database);
    }

    @Override
    public void stop() {
        logger.info("Stopping MongoDB store service");
        try {
            mongoClient.close();
        } catch (Exception e) {
            logger.error("Error closing MongoDB client", e);
        }
    }
}
