package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.*;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * A standard MongoDB based implementation of {@link WorkflowTrigger}.
 */
public class WorkflowTriggerStoreImpl implements WorkflowTriggerStore {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowTriggerStoreImpl.class);

    private static final String COLLECTION_NAME = "workflowtriggers";

    private final CodecRegistry pojoCodecRegistry;
    private final MongoClient mongoClient;

    WorkflowTriggerStoreImpl(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        ClassModelBuilder<WorkflowTrigger> workflowTriggerClassModelBuilder =
                ClassModel.builder(WorkflowTrigger.class);
        ClassModel<WorkflowTrigger> workflowTriggerClassModel = workflowTriggerClassModelBuilder.build();
        ClassModel<Schedule> scheduleClassModel =
                ClassModel.builder(Schedule.class).enableDiscriminator(true).build();
        ClassModel<SimpleSchedule> simpelScheduleClassModel =
                ClassModel.builder(SimpleSchedule.class).enableDiscriminator(true).build();
        ClassModel<DailyTimeIntervalSchedule> dailyTimeIntClassModel =
                ClassModel.builder(DailyTimeIntervalSchedule.class).enableDiscriminator(true).build();
        ClassModel<CronSchedule> cronScheduleClassModel =
                ClassModel.builder(CronSchedule.class).enableDiscriminator(true).build();
        ClassModel<CalendarIntervalSchedule> calenderScheduleClassModel =
                ClassModel.builder(CalendarIntervalSchedule.class).enableDiscriminator(true).build();
        PojoCodecProvider codecProvider = PojoCodecProvider.builder().automatic(true)
                .register(workflowTriggerClassModel, scheduleClassModel,
                        simpelScheduleClassModel, dailyTimeIntClassModel,
                        cronScheduleClassModel, calenderScheduleClassModel).build();
        pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                fromProviders(codecProvider));
    }

    @Override
    public List<WorkflowTrigger> load(String namespace) {
        logger.debug("load : WorkflowTrigger loaded to List with namespace {}", namespace);
        MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(namespace);
        List<WorkflowTrigger> list = new ArrayList<>();
        FindIterable<WorkflowTrigger> workflowTriggers = mongoCollection.find(WorkflowTrigger.class);
        for (WorkflowTrigger workflowTrigger : workflowTriggers) {
            list.add(workflowTrigger);
        }
        logger.debug("load : Returned {} workflow triggers for namespace {}", list.size(), namespace);
        return list;
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowName(String namespace, String workflowName) {
        logger.debug("loadByWorkflowName : WorkflowTrigger for {} loaded to List with namespace {}",
                workflowName, namespace);
        MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(namespace);
        List<WorkflowTrigger> list = new ArrayList<>();
        FindIterable<WorkflowTrigger> workflowtriggers;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName);
        workflowtriggers = mongoCollection.find(query);
        for (WorkflowTrigger workflowtrigger : workflowtriggers) {
            list.add(workflowtrigger);
        }
        logger.debug("loadByWorkflowName : Returned {} workflow triggers name {} for namespace {}",
                list.size(), workflowName, namespace);
        return list;
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowNameAndEnabled(String namespace,
                                                              String workflowName, boolean enabled) {
        logger.debug("loadByWorkflowNameAndEnabled : WorkflowTrigger for {} loaded to List with namespace {}",
                workflowName, namespace);
        MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(namespace);
        ArrayList<WorkflowTrigger> list = new ArrayList<>();
        FindIterable<WorkflowTrigger> workflowtriggers;
        BasicDBObject query = new BasicDBObject("namespace", namespace)
                .append("workflow", workflowName).append("enabled", enabled);
        workflowtriggers = mongoCollection.find(query);
        for (WorkflowTrigger workflowtrigger : workflowtriggers) {
            list.add(workflowtrigger);
        }
        logger.debug("loadByWorkflowNameAndEnabled : Returned {} workflow triggers name {} for namespace {}",
                list.size(), workflowName, namespace);
        return list;
    }

    @Override
    public void store(WorkflowTrigger workflowTrigger) {
        logger.debug("store : WorkflowTrigger {} for {} loaded to List with namespace {}",
                workflowTrigger.getName(), workflowTrigger.getWorkflow(), workflowTrigger.getNamespace());
        MongoCollection<WorkflowTrigger> workflowTriggerCollection =
                getMongoCollection(workflowTrigger.getNamespace());
        workflowTriggerCollection.insertOne(workflowTrigger);
        logger.debug("store : WorkflowTrigger {} stored for namespace {}",
                workflowTrigger.getName(), workflowTrigger.getNamespace());
    }

    @Override
    public WorkflowTrigger load(WorkflowTriggerId workflowTriggerId) {
        logger.debug("load : WorkflowTrigger {} for {} loaded with namespace {}",
                workflowTriggerId.getName(), workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace());
        MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(workflowTriggerId.getNamespace());
        return mongoCollection.find(eq("name", workflowTriggerId.getName())).first();
    }

    @Override
    public void update(WorkflowTrigger workflowTrigger) {
        logger.debug("update : updated WorkflowTrigger {} for {}  with namespace {}",
                workflowTrigger.getName(), workflowTrigger.getWorkflow(), workflowTrigger.getNamespace());
        MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(workflowTrigger.getNamespace());
        mongoCollection.findOneAndUpdate(
                eq("name", workflowTrigger.getName()), (set("workflow", workflowTrigger.getWorkflow())));
    }

    @Override
    public void delete(WorkflowTriggerId workflowTriggerId) {
        logger.debug("delete : WorkflowTrigger {} for {} deleted with namespace {}",
                workflowTriggerId.getName(), workflowTriggerId.getWorkflow(), workflowTriggerId.getNamespace());
        MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(workflowTriggerId.getNamespace());
        mongoCollection.deleteOne(eq("name", workflowTriggerId.getName()));
    }

    private MongoCollection<WorkflowTrigger> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION_NAME, WorkflowTrigger.class);
    }
}
