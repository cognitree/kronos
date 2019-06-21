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

package com.cognitree.kronos.queue;

import com.cognitree.kronos.queue.consumer.Consumer;
import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.Producer;
import com.cognitree.kronos.queue.producer.ProducerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

public class QueueTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final String TOPIC_A = "topicA";
    private static final String TOPIC_B = "topicB";

    private static Producer topicAProducer;
    private static Consumer topicAConsumer;

    private static Producer topicBProducer;
    private static Consumer topicBConsumer;

    @Before
    public void init() throws Exception {
        final InputStream queueConfigAsStream =
                QueueTest.class.getClassLoader().getResourceAsStream("queue.yaml");
        QueueConfig queueConfig = MAPPER.readValue(queueConfigAsStream, QueueConfig.class);
        initProducer(queueConfig.getProducerConfig());
        initConsumer(queueConfig.getConsumerConfig());
        Thread.sleep(2000);
    }

    private void initProducer(ProducerConfig producerConfig) throws Exception {
        topicAProducer = (Producer) Class.forName(producerConfig.getProducerClass())
                .getConstructor()
                .newInstance();
        topicAProducer.init(TOPIC_A, producerConfig.getConfig());

        topicBProducer = (Producer) Class.forName(producerConfig.getProducerClass())
                .getConstructor()
                .newInstance();
        topicBProducer.init(TOPIC_B, producerConfig.getConfig());
    }

    private void initConsumer(ConsumerConfig consumerConfig) throws Exception {
        topicAConsumer = (Consumer) Class.forName(consumerConfig.getConsumerClass())
                .getConstructor()
                .newInstance();
        topicAConsumer.init(TOPIC_A, consumerConfig.getConfig());
        for (int i = 0; i < 2; i++) { // for kafka consumer to poll all the messages before we start the test
            topicAConsumer.poll();
        }

        topicBConsumer = (Consumer) Class.forName(consumerConfig.getConsumerClass())
                .getConstructor()
                .newInstance();
        topicBConsumer.init(TOPIC_B, consumerConfig.getConfig());
        for (int i = 0; i < 2; i++) { // for kafka consumer to poll all the messages before we start the test
            topicBConsumer.poll();
        }
    }

    @After
    public void destroy() {
        topicAProducer.close();
        topicBProducer.close();

        topicAConsumer.close();
        topicBConsumer.close();

        topicAConsumer.destroy();
        topicBConsumer.destroy();
    }

    @Test
    public void testProducerAndConsumerSingleTopic() throws InterruptedException {
        LinkedList<String> records = new LinkedList<>();
        records.add("record1");
        records.add("record2");
        records.add("record3");
        records.add("record4");
        records.forEach(record -> topicAProducer.send(record));
        Thread.sleep(2000);

        List<String> recordsFromConsumer = topicAConsumer.poll(4);
        for (int i = 0; i < 2; i++) {
            recordsFromConsumer.addAll(topicAConsumer.poll());
            Thread.sleep(500);
        }
        Assert.assertTrue("Records sent " + records + " and records consumed" + recordsFromConsumer + " do not match",
                recordsFromConsumer.size() == records.size() &&
                        recordsFromConsumer.containsAll(records) && records.containsAll(recordsFromConsumer));
    }

    @Test
    public void testProducerAndConsumerMultipleTopic() throws InterruptedException {
        LinkedList<String> recordsForA = new LinkedList<>();
        recordsForA.add("record1");
        recordsForA.add("record2");
        recordsForA.add("record3");
        recordsForA.add("record4");
        recordsForA.forEach(record -> topicAProducer.send(record));

        LinkedList<String> recordsForB = new LinkedList<>();
        recordsForB.add("record1");
        recordsForB.add("record2");
        recordsForB.add("record3");
        recordsForB.add("record4");
        recordsForB.forEach(record -> topicBProducer.send(record));

        Thread.sleep(500);

        List<String> recordsFromConsumerTopicA = topicAConsumer.poll(4);
        for (int i = 0; i < 2; i++) {
            recordsFromConsumerTopicA.addAll(topicAConsumer.poll());
            Thread.sleep(500);
        }
        Assert.assertTrue("Records sent " + recordsForA + " and records consumed" + recordsFromConsumerTopicA + " do not match",
                recordsFromConsumerTopicA.size() == recordsForA.size() &&
                        recordsFromConsumerTopicA.containsAll(recordsForA) && recordsForA.containsAll(recordsFromConsumerTopicA));

        List<String> recordsFromConsumerTopicB = topicBConsumer.poll(4);
        for (int i = 0; i < 2; i++) {
            recordsFromConsumerTopicB.addAll(topicBConsumer.poll());
            Thread.sleep(500);
        }
        Assert.assertTrue("Records sent " + recordsForB + " and records consumed" + recordsFromConsumerTopicB + " do not match",
                recordsFromConsumerTopicB.size() == recordsForB.size() &&
                        recordsFromConsumerTopicB.containsAll(recordsForB) && recordsForB.containsAll(recordsFromConsumerTopicB));
    }

    @Test
    public void testProducerAndConsumerSingleTopicInOrder() throws InterruptedException {
        LinkedList<String> records = new LinkedList<>();
        records.add("record1");
        records.add("record2");
        records.add("record3");
        records.add("record4");
        records.forEach(record -> topicAProducer.sendInOrder(record, "orderingKey"));
        Thread.sleep(2000);

        List<String> recordsFromConsumer = topicAConsumer.poll(4);
        for (int i = 0; i < 2; i++) {
            recordsFromConsumer.addAll(topicAConsumer.poll());
            Thread.sleep(500);
        }
        Assert.assertEquals(records, recordsFromConsumer);
    }

    @Test
    public void testProducerAndConsumerMultipleTopicInOrder() throws InterruptedException {
        LinkedList<String> recordsForA = new LinkedList<>();
        recordsForA.add("record1");
        recordsForA.add("record2");
        recordsForA.add("record3");
        recordsForA.add("record4");
        recordsForA.forEach(record -> topicAProducer.sendInOrder(record, "orderingKey"));
        Thread.sleep(2000);

        LinkedList<String> recordsForB = new LinkedList<>();
        recordsForB.add("record1");
        recordsForB.add("record2");
        recordsForB.add("record3");
        recordsForB.add("record4");
        recordsForB.forEach(record -> topicBProducer.sendInOrder(record, "orderingKey"));

        List<String> recordsFromConsumerTopicA = topicAConsumer.poll(4);
        for (int i = 0; i < 2; i++) {
            recordsFromConsumerTopicA.addAll(topicAConsumer.poll());
            Thread.sleep(500);
        }
        Assert.assertEquals(recordsForA, recordsFromConsumerTopicA);

        List<String> recordsFromConsumerTopicB = topicBConsumer.poll(4);
        for (int i = 0; i < 2; i++) {
            recordsFromConsumerTopicB.addAll(topicBConsumer.poll());
            Thread.sleep(500);
        }
        Assert.assertEquals(recordsForB, recordsFromConsumerTopicB);
    }
}
