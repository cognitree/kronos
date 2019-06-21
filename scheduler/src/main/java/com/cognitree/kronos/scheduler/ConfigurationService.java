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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.events.ConfigUpdate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A service that consumes config updates from a specified queue and processes them.
 */
public class ConfigurationService implements Service {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationService.class);

    private static final ObjectReader READER = new ObjectMapper().reader().forType(ConfigUpdate.class);

    // used in junit
    final String configurationQueue;

    private final ConsumerConfig consumerConfig;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private Consumer configurationConsumer;
    private long pollIntervalInMs;

    ConfigurationService(QueueConfig queueConfig) {
        if (queueConfig.getConfigurationQueue() == null || queueConfig.getConsumerConfig() == null) {
            logger.error("missing one or more mandatory configuration: configurationQueue/ consumerConfig ");
            throw new IllegalArgumentException("missing one or more mandatory configuration: " +
                    "configurationQueue/ consumerConfig");
        }
        this.consumerConfig = queueConfig.getConsumerConfig();
        this.configurationQueue = queueConfig.getConfigurationQueue();
        this.pollIntervalInMs = queueConfig.getPollIntervalInMs();
    }

    public static ConfigurationService getService() {
        return (ConfigurationService) ServiceProvider.getService(ConfigurationService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        logger.info("init: Initializing configuration service");
        initConsumer();
    }

    @Override
    public void start() {
        logger.info("start: Starting configuration service");
        ServiceProvider.registerService(this);
        scheduledExecutorService.scheduleAtFixedRate(this::failSafeProcessUpdates,
                pollIntervalInMs, pollIntervalInMs, MILLISECONDS);
    }

    @Override
    public void stop() {

    }

    private void initConsumer() throws Exception {
        logger.info("Initializing configuration consumer with config {}", consumerConfig);
        configurationConsumer = (Consumer) Class.forName(consumerConfig.getConsumerClass())
                .getConstructor()
                .newInstance();
        configurationConsumer.init(configurationQueue, consumerConfig.getConfig());
    }

    /**
     * A wrapper method to prevent task the cancellation due to unhandled exceptions
     */
    private void failSafeProcessUpdates() {
        try {
            processUpdates();
        } catch (Exception ex) {
            logger.error("failSafeProcessUpdates : Unexpected exception " +
                    "occurred while processing config updates: " + ex.getMessage(), ex);
        }
    }

    /**
     * Poll, parse and process updates from the configured queue.
     */
    private void processUpdates() {
        final List<String> configUpdates = configurationConsumer.poll();
        for (String configUpdateAsString : configUpdates) {
            if (configUpdateAsString.trim().isEmpty())
                logger.trace("processUpdates: quietly skipping over empty config update...");
            try {
                final ConfigUpdate configUpdate = READER.readValue(configUpdateAsString.trim());
                processUpdate(configUpdate);
            } catch (IOException e) {
                // Do not throw the exception but continue to process other updates
                logger.error("processUpdates: unable to parse the config update received: " + configUpdateAsString, e);
            } catch (UnsupportedOperationException | ServiceException | ValidationException | SchedulerException e) {
                // Do not throw the exception but continue to process other updates
                logger.error("processUpdates: error occurred in processing the config update received: {}",
                        configUpdateAsString, e);
            }
        }
    }

    /**
     * Delegate the config update to the appropriate service
     *
     * @param configUpdate the configuration update
     * @throws ServiceException, ValidationException, SchedulerException: exceptions thrown by the services
     */
    private void processUpdate(ConfigUpdate configUpdate)
            throws ServiceException, ValidationException, SchedulerException {
        if (configUpdate.getModel() instanceof Namespace) {
            processUpdate(configUpdate.getAction(), (Namespace) configUpdate.getModel());
        } else if (configUpdate.getModel() instanceof Workflow) {
            processUpdate(configUpdate.getAction(), (Workflow) configUpdate.getModel());
        } else if (configUpdate.getModel() instanceof WorkflowTrigger) {
            processUpdate(configUpdate.getAction(), (WorkflowTrigger) configUpdate.getModel());
        } else {
            logger.error("processUpdate : received an unhandled model object in the config update : {}", configUpdate);
            throw new UnsupportedOperationException("Unsupported entity : " + configUpdate.getModel());
        }
    }

    /**
     * Process config updates to Namespaces
     */
    private void processUpdate(ConfigUpdate.Action action, Namespace namespace)
            throws ServiceException, ValidationException {
        switch (action) {
            case create:
                NamespaceService.getService().add(namespace);
                break;
            case delete:
                logger.error("processUpdate : delete is not support for a namespace : {}", namespace);
                throw new UnsupportedOperationException("Delete is not support for a namespace");
            case update:
                NamespaceService.getService().update(namespace);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported action " + action + " for a namespace");
        }
    }

    /**
     * Process config updates to Workflows
     */
    private void processUpdate(ConfigUpdate.Action action, Workflow workflow)
            throws ValidationException, ServiceException, SchedulerException {
        switch (action) {
            case create:
                WorkflowService.getService().add(workflow);
                break;
            case delete:
                WorkflowService.getService().delete(workflow);
                break;
            case update:
                WorkflowService.getService().update(workflow);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported action " + action + " for a workflow");
        }
    }

    /**
     * Process config updates to WorkflowTriggers
     */
    private void processUpdate(ConfigUpdate.Action action, WorkflowTrigger trigger)
            throws SchedulerException, ServiceException, ValidationException {
        switch (action) {
            case create:
                WorkflowTriggerService.getService().add(trigger);
                break;
            case delete:
                WorkflowTriggerService.getService().delete(trigger);
                break;
            case update:
                if (trigger.isEnabled())
                    WorkflowTriggerService.getService().resume(trigger);
                else
                    WorkflowTriggerService.getService().pause(trigger);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported action " + action + " for a workflow trigger");
        }
    }
}
