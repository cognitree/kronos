package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.queue.QueueConfig;
import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.scheduler.events.ConfigUpdate;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
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
 *
 */
public class ConfigurationService implements Service {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationService.class);

    public static ConfigurationService getService() {
        return (ConfigurationService) ServiceProvider.getService(ConfigurationService.class.getSimpleName());
    }

    private final ConsumerConfig consumerConfig;
    final String configurationQueue;

    private Consumer consumer;
    private long pollInterval;

    private static final ObjectReader READER = new ObjectMapper()
            .reader().forType(ConfigUpdate.class);

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(1);

    ConfigurationService(QueueConfig queueConfig) {
        this.consumerConfig = queueConfig.getConsumerConfig();
        this.configurationQueue = queueConfig.getConfigurationQueue();
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
        scheduledExecutorService.scheduleAtFixedRate(this::failSafeProcessUpdates, pollInterval, pollInterval, MILLISECONDS);
    }

    @Override
    public void stop() {

    }

    private void initConsumer() throws Exception {
        logger.info("Initializing consumer with config {}", consumerConfig);
        consumer = (Consumer) Class.forName(consumerConfig.getConsumerClass())
                .getConstructor()
                .newInstance();
        consumer.init(consumerConfig.getConfig());
        pollInterval = consumerConfig.getPollIntervalInMs();
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
        final List<String> configUpdates = consumer.poll(configurationQueue);
        for (String configUpdateAsString : configUpdates) {
            if (configUpdateAsString.trim().isEmpty())
                logger.trace("processUpdates: quietly skipping over empty config update...");
            try {
                ConfigUpdate configUpdate = READER.readValue(configUpdateAsString.trim());
                processUpdate(configUpdate);
            } catch (IOException e) {
                // Do not throw the exception but continue to process other updates
                logger.error("processUpdates: unable to parse the config update received: " + configUpdateAsString, e);
            } catch (UnsupportedOperationException | ServiceException | ValidationException | SchedulerException e) {
                // Do not throw the exception but continue to process other updates
                logger.error("processUpdates: error occurred in processing the config update received: " + configUpdateAsString, e);
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
            processNamespaceUpdate(configUpdate);
        } else if (configUpdate.getModel() instanceof Workflow) {
            processWorkflowUpdate(configUpdate);
        } else if (configUpdate.getModel() instanceof WorkflowTrigger) {
            processWorkflowTriggerUpdate(configUpdate);
        } else {
            logger.error("processUpdate : received an unhandled model object in the config update : {}", configUpdate);
            throw new UnsupportedOperationException("Unsupported entity : " + configUpdate.getModel());
        }
    }

    /**
     * Process config updates to Namespaces
     */
    private void processNamespaceUpdate(ConfigUpdate configUpdate)
            throws ServiceException, ValidationException {
        Namespace namespace = (Namespace) configUpdate.getModel();
        switch (configUpdate.getAction()) {
            case create:
                NamespaceService.getService().add(namespace);
                break;
            case delete:
                logger.error("processUpdate : delete is not support for a namespace : {}", configUpdate);
                throw new UnsupportedOperationException("Delete is not support for a namespace");
            case update:
            default:
                NamespaceService.getService().update(namespace);
                break;
        }
    }

    /**
     * Process config updates to Workflows
     */
    private void processWorkflowUpdate(ConfigUpdate configUpdate)
            throws ValidationException, ServiceException, SchedulerException {
        Workflow workflow = (Workflow) configUpdate.getModel();
        switch (configUpdate.getAction()) {
            case create:
                WorkflowService.getService().add(workflow);
                break;
            case delete:
                WorkflowService.getService().delete(workflow);
                break;
            case update:
            default:
                WorkflowService.getService().update(workflow);
                break;
        }
    }

    /**
     * Process config updates to WorkflowTriggers
     */
    private void processWorkflowTriggerUpdate(ConfigUpdate configUpdate)
            throws SchedulerException, ServiceException, ValidationException {
        WorkflowTrigger trigger = (WorkflowTrigger) configUpdate.getModel();
        switch (configUpdate.getAction()) {
            case create:
                WorkflowTriggerService.getService().add(trigger);
                break;
            case delete:
                WorkflowTriggerService.getService().delete(trigger);
                break;
            case update:
            default:
                if (trigger.isEnabled())
                    WorkflowTriggerService.getService().resume(trigger);
                else
                    WorkflowTriggerService.getService().pause(trigger);
        }
    }
}
