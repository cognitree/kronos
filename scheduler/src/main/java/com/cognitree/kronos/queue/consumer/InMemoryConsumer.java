package com.cognitree.kronos.queue.consumer;

import com.cognitree.kronos.queue.InMemoryQueueFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryConsumer.class);

    @Override
    public void init(ObjectNode config) {
        logger.info("Initializing consumer for in-memory queue with config {}", config);
    }

    @Override
    public List<String> poll(String topic) {
        return poll(topic, Integer.MAX_VALUE);
    }

    @Override
    public List<String> poll(String topic, int size) {
        logger.trace("Received request to poll messages from topic {} with max size {}", topic, size);
        final LinkedBlockingQueue<String> blockingQueue = InMemoryQueueFactory.getQueue(topic);
        List<String> records = new ArrayList<>();
        while (!blockingQueue.isEmpty() && records.size() < size)
            records.add(blockingQueue.poll());
        return records;
    }

    @Override
    public void close() {

    }
}
