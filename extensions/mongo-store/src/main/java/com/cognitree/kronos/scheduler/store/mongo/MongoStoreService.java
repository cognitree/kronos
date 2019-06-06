/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.model.Policy;
import com.cognitree.kronos.model.RetryPolicy;
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
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.novemberain.quartz.mongodb.MongoDBJobStore;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * A standard MongoDB connection based implementation of {@link StoreService}.
 */
public class MongoStoreService extends StoreService {

    private static final Logger logger = LoggerFactory.getLogger(MongoStoreService.class);

    private static final String CONNECTION_STRING = "connectionString";
    private static final String DEFAULT_CONNECTION_STRING = "mongodb://localhost:27017";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String AUTH_DATABASE = "authDatabase";
    private static final String DEFAULT_AUTH_DATABASE = "admin";
    private static final String SCHEDULER_DATABASE = "schedulerDatabase";
    private static final String DEFAULT_QUARTZ_DATABASE = "quartz";

    private String connectionString;
    private MongoCredential credential;
    private String quartzDatabase;
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
        if (config != null && config.hasNonNull(CONNECTION_STRING)) {
            connectionString = config.get(CONNECTION_STRING).asText();
        } else {
            connectionString = DEFAULT_CONNECTION_STRING;
        }
        if (config != null && config.hasNonNull(USER) && config.hasNonNull(PASSWORD)) {
            String authDatabase;
            if (config.hasNonNull(AUTH_DATABASE)) {
                authDatabase = config.get(AUTH_DATABASE).asText();
            } else {
                authDatabase = DEFAULT_AUTH_DATABASE;
            }
            credential = MongoCredential.createCredential(config.get(USER).asText(), authDatabase,
                    config.get(PASSWORD).asText().toCharArray());
        }
        if (config != null && config.hasNonNull(SCHEDULER_DATABASE)) {
            quartzDatabase = config.get(SCHEDULER_DATABASE).asText();
        } else {
            quartzDatabase = DEFAULT_QUARTZ_DATABASE;
        }
    }

    private void initMongoClient() {
        final PojoCodecProvider customCodecProvider = PojoCodecProvider.builder().automatic(true)
                .register(ClassModel.builder(Schedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(SimpleSchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(CronSchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(FixedDelaySchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(DailyTimeIntervalSchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(CalendarIntervalSchedule.class).enableDiscriminator(true).build(),
                        ClassModel.builder(Policy.class).enableDiscriminator(true).build(),
                        ClassModel.builder(RetryPolicy.class).enableDiscriminator(true).build())
                .build();

        final CodecRegistry codecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(customCodecProvider),
                fromCodecs(new JobStatusCodec()),
                fromCodecs(new TaskStatusCodec()),
                fromProviders(new DocumentCodecProvider()),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientSettings.Builder clusterSettings = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.applyConnectionString(new ConnectionString(connectionString)))
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
        quartzJobStore = new MongoDBJobStore(mongoClient.getDatabase(quartzDatabase));
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
