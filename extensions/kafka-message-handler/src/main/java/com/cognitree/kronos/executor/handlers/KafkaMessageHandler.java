package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.executor.model.TaskResult;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * A {@link TaskHandler} implementation to push a message to a kafka topic.
 */
public class KafkaMessageHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageHandler.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String KAFKA_PRODUCER_CONFIG_KEY = "kafkaProducerConfig";
    // topic to push the message
    private static final String TOPIC_KEY = "topic";
    // message to push to kafka topic
    private static final String MESSAGE_KEY = "message";

    private String defaultTopic;
    private KafkaProducer<String, String> kafkaProducer;

    @Override
    public void init(ObjectNode config) {
        logger.info("Initializing producer for kafka with config {}", config);
        if (config == null || !config.hasNonNull(KAFKA_PRODUCER_CONFIG_KEY)) {
            throw new IllegalArgumentException("missing mandatory configuration: [kafkaProducerConfig]");
        }
        Properties kafkaProducerConfig = OBJECT_MAPPER.convertValue(config.get(KAFKA_PRODUCER_CONFIG_KEY), Properties.class);
        kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
        defaultTopic = config.get(TOPIC_KEY).asText();
    }

    @Override
    public TaskResult handle(Task task) {
        logger.info("Received request to handle task {}", task);
        final Map<String, Object> taskProperties = task.getProperties();
        final String topic = (String) taskProperties.getOrDefault(TOPIC_KEY, defaultTopic);
        try {
            final Object message = taskProperties.get(MESSAGE_KEY);
            send(topic, OBJECT_MAPPER.writeValueAsString(message));
            return TaskResult.SUCCESS;
        } catch (JsonProcessingException e) {
            logger.error("error parsing task properties {}", taskProperties, e);
            return new TaskResult(false, "Error parsing task properties : " + e.getMessage());
        }
    }

    private void send(String topic, String record) {
        logger.trace("Received request to send message {} to topic {}.", record, topic);
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, record);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending record {} over kafka to topic {}.",
                        record, topic, exception);
            }
        });
    }
}
