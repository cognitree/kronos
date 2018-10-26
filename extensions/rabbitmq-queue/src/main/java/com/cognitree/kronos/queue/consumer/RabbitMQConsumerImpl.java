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
import java.util.concurrent.atomic.AtomicBoolean;

public class RabbitMQConsumerImpl implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DEFAULT_EXCHANGE_NAME = "kronos_exchange";

    private Channel channel;
    private Connection connection;
    private Properties rabbitmqConsumerConfig;
    private ConcurrentHashMap<String, LinkedBlockingQueue<String>> messages = new ConcurrentHashMap<>();

    public void init(ObjectNode config) {
        logger.info("init: Initializing consumer for rabbitmq with config {}", config);
        rabbitmqConsumerConfig = OBJECT_MAPPER.convertValue(config.get("rabbitmqConsumerConfig"), Properties.class);
        // force override consumer configuration for rabbitmq to poll max 1 message at a time
        rabbitmqConsumerConfig.put("max.poll.records", 1);
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

    private void initConsumer(String topic) {
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
                    AtomicBoolean lock = new AtomicBoolean(messages.get(topic) == null);
                    if (lock.compareAndSet(true, false)) {
                        LinkedBlockingQueue<String> newTopicQueue = new LinkedBlockingQueue<>();
                        newTopicQueue.add(message);
                        messages.put(topic, newTopicQueue);
                    } else {
                        LinkedBlockingQueue<String> topicMessages = messages.get(topic);
                        topicMessages.add(message);
                    }
                    try {
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };
//            channel.basicConsume(topic, false, consumer);
            logger.info("initConsumer: Created a consumer for the topic " + topic);
        } catch (IOException e) {
            logger.error("initConsumer: Unable to create a consumer for the topic " + topic, e);
        }
    }

    private void ensureQueueExists(String topic, String exchangeName) throws IOException {
        channel.queueDeclare(topic, true, false, false, null);
        channel.queueBind(topic, exchangeName, topic);
    }

    @Override
    public List<String> poll(String topic) {
        return poll(topic, 100000);
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
                initConsumer(topic);
            }
            LinkedBlockingQueue<String> tempQueue = messages.get(topic);
            while (size-- > 0) {
                GetResponse response = channel.basicGet(topic, false);
                if (response != null) {
                    AMQP.BasicProperties properties = response.getProps();
                    byte[] body = response.getBody();
                    long deliveryTag = response.getEnvelope().getDeliveryTag();
                    String message = new String(body);
                    tasks.add(message);
                    channel.basicAck(deliveryTag, false);
                }
            }
//            tempQueue.drainTo(tasks, size);
        } catch (Exception e) {
            logger.error("poll: Unable to drain the messages from Queue for the topic " + topic, e);
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
            connection.close();
        } catch (Exception e) {
            logger.error("close: Failed to close the connection to the channel : " + e.getMessage(), e);
        }
        logger.info("close: Closed the connection to the factory");
    }
}
