package com.cognitree.kronos.queue.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RabbitMQConsumerImpl implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DEFAULT_EXCHANGE_NAME = "kronos_exchange";

    private Channel channel;
    private Connection connection;
    private Properties rabbitmqConsumerConfig;

    @Override
    public void init(ObjectNode config) {
        logger.info("init: Initializing consumer for rabbitmq with config {}", config);
        rabbitmqConsumerConfig = OBJECT_MAPPER.convertValue(config.get("rabbitmqConsumerConfig"), Properties.class);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmqConsumerConfig.getProperty("hostname"));
        factory.setPort(Integer.parseInt(rabbitmqConsumerConfig.getProperty("port")));
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            String exchangeName = rabbitmqConsumerConfig.getProperty("exchangeName", DEFAULT_EXCHANGE_NAME);
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, true);
        } catch (Exception e) {
            logger.error("init: Consumer is unable to connect to the RabbitMQ sever", e);
        }
    }

    @Override
    public List<String> poll(String topic) {
        int maxPollsize= Integer.parseInt(rabbitmqConsumerConfig.getProperty("maxPollMessages",
                String.valueOf(1000)));
        return poll(topic, maxPollsize);
    }

    @Override
    public List<String> poll(String topic, int size) {
        logger.trace("poll: Received request to poll messages from topic {} with max size {}", topic, size);
        List<String> tasks = new ArrayList<>();
        if (topic == null) {
            return tasks;
        }
        try {
            String exchangeName = rabbitmqConsumerConfig.getProperty("exchangeName", DEFAULT_EXCHANGE_NAME);
            ensureQueueExists(topic, exchangeName);
            while (size-- > 0) {
                GetResponse response = channel.basicGet(topic, false);
                if (response != null) {
                    byte[] body = response.getBody();
                    long deliveryTag = response.getEnvelope().getDeliveryTag();
                    String message = new String(body);
                    tasks.add(message);
                    channel.basicAck(deliveryTag, false);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("poll: Unable to drain the messages from Queue for the topic " + topic, e);
        }
        return tasks;
    }

    @Override
    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (Exception e) {
            logger.error("close: Failed to close the connection to the channel : " + e.getMessage(), e);
        }
        logger.info("close: Closed the connection to the factory");
    }

    private void ensureQueueExists(String topic, String exchangeName) throws IOException {
        channel.queueDeclare(topic, true, false, false, null);
        channel.queueBind(topic, exchangeName, topic);
    }
}
