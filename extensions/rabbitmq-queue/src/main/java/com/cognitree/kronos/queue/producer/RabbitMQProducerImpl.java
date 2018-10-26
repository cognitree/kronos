package com.cognitree.kronos.queue.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducerImpl implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DEFAULT_EXCHANGE_NAME = "kronos_exchange";

    private Channel channel;
    private Properties rabbitmqProducerConfig;

    public void init(ObjectNode config) {
        logger.info("init: Initializing producer for rabbitmq with config {}", config);
        rabbitmqProducerConfig = OBJECT_MAPPER.convertValue(config.get("rabbitmqProducerConfig"), Properties.class);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(String.valueOf(rabbitmqProducerConfig.get("hostname")));
        factory.setPort(Integer.parseInt(rabbitmqProducerConfig.get("port").toString()));
        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
            String exchangeName = rabbitmqProducerConfig.getProperty("exchangeName", DEFAULT_EXCHANGE_NAME);
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, true);
        } catch (IOException | TimeoutException e) {
            logger.error("init: Unable to connect to the RabbitMQ sever", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(String topic, String record) {
        logger.trace("send: Received request to send message {} to topic {}.", record, topic);
        String exchangeName = rabbitmqProducerConfig.getProperty("exchangeName", DEFAULT_EXCHANGE_NAME);
        try {
            ensureQueueExists(topic, exchangeName);
            AMQP.BasicProperties messageProperties = new AMQP.BasicProperties().builder().deliveryMode(2).build();
            channel.basicPublish(exchangeName, topic, messageProperties, record.getBytes("UTF-8"));
            logger.info("send: Delivered message {} to the exchange '{}' with routing key '{}'.", record, exchangeName, topic);
        } catch (Exception e) {
            logger.error("send: Failed to deliver the message to the exchange {}.", exchangeName, e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
            channel.getConnection().close();
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
