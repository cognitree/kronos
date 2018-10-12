package com.cognitree.kronos.queue.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class RabbitMQConsumerImpl implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DEFAULT_EXCHANGE_NAME = "kronos_exchange";

    private Channel channel;
    private Properties rabbitmqConsumerConfig;
    private ConcurrentHashMap<String, LinkedBlockingQueue<String>> messages = new ConcurrentHashMap<>();

    public void init(ObjectNode config) {
        logger.info("init: Initializing consumer for rabbitmq with config {}", config);
        rabbitmqConsumerConfig = OBJECT_MAPPER.convertValue(config.get("rabbitmqConsumerConfig"), Properties.class);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmqConsumerConfig.getProperty("hostname"));
        factory.setPort(Integer.parseInt(rabbitmqConsumerConfig.getProperty("port")));
        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
            String exchangeName = rabbitmqConsumerConfig.getProperty("exchangeName", DEFAULT_EXCHANGE_NAME);
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, true);
        } catch (Exception e) {
            logger.error("init: Consumer is unable to connect to the RabbitMQ sever", e);
        }
    }

    @Override
    public void initTopic(String topic) {
        String exchangeName = rabbitmqConsumerConfig.getProperty("exchangeName", DEFAULT_EXCHANGE_NAME);
        int prefetchCount = Integer.parseInt(rabbitmqConsumerConfig.getProperty("prefetchCount"));
        try {
            ensureQueueExists(topic, exchangeName);
            channel.basicQos(prefetchCount);
            DefaultConsumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) {
                    String message = new String(body, StandardCharsets.UTF_8);
                    if (messages.get(topic) == null) {
                        LinkedBlockingQueue<String> newQueue = new LinkedBlockingQueue<>();
                        newQueue.add(message);
                        messages.put(topic, newQueue);
                    } else {
                        LinkedBlockingQueue<String> topicMessages = messages.get(topic);
                        topicMessages.add(message);
                    }
                }
            };
            channel.basicConsume(topic, true, consumer);
        } catch (IOException e) {
            logger.error("initTopic: Unable to create a consumer for the topic " + topic, e);
            throw new RuntimeException(e);
        }
    }

    private void ensureQueueExists(String topic, String exchangeName) throws IOException {
        channel.queueDeclare(topic, true, false, false, null);
        channel.queueBind(topic, exchangeName, topic);
    }

    @Override
    public List<String> poll(String topic) {
        return poll(topic, Integer.MAX_VALUE);
    }

    @Override
    public List<String> poll(String topic, int size) {
        logger.trace("poll: Received request to poll messages from topic {} with max size {}", topic, size);
        List<String> tasks = new ArrayList<>();
        if (topic == null) {
            return tasks;
        }
        try {
            if (!messages.containsKey(topic)) {
                createRabbitMQConsumerTopic(topic);
            }
            LinkedBlockingQueue<String> tempQueue = messages.get(topic);
            tempQueue.drainTo(tasks, size);
        } catch (Exception e) {
            logger.error("poll: Unable to drain the messages from Queue from the HashMap for the topic " + topic, e);
        }
        return tasks;
    }

    private synchronized void createRabbitMQConsumerTopic(String topic) {
        if (!messages.containsKey(topic)) {
            logger.info("createRabbitMQConsumerTopic: Creating rabbitmq consumer for topic {}", topic);
            messages.put(topic, new LinkedBlockingQueue<>());
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
}
