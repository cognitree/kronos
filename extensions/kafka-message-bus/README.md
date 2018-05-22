# Kafka Message Bus

Kafka Message Bus is an extension for message bus which uses [Kafka](http://kafka.apache.org) to exchange to message between scheduler and executor.

## Configuring Kafka Message Bus

Update the `app.yaml` to use Kafka as message bus
```
# configure task producer
taskProducerConfig:
  producerClass: com.cognitree.kronos.queue.producer.KafkaTaskProducer
  config:
    producerConfig:
      bootstrap.servers : localhost:9092
      key.serializer : org.apache.kafka.common.serialization.StringSerializer
      value.serializer : org.apache.kafka.common.serialization.StringSerializer

# configure task consumer
taskConsumerConfig:
  consumerClass: com.cognitree.kronos.queue.consumer.KafkaTaskConsumer
  config:
    consumerConfig:
      bootstrap.servers : localhost:9092
      group.id: schedulerframework
      key.deserializer : org.apache.kafka.common.serialization.StringDeserializer
      value.deserializer : org.apache.kafka.common.serialization.StringDeserializer
    taskTopics: typeA, typeB, shell
    pollTimeout: 5s
    pollInterval: 5s

# configure task status producer
taskStatusProducerConfig:
  producerClass: com.cognitree.kronos.queue.producer.KafkaTaskStatusProducer
  config:
    producerConfig:
      bootstrap.servers : localhost:9092
      key.serializer : org.apache.kafka.common.serialization.StringSerializer
      value.serializer : org.apache.kafka.common.serialization.StringSerializer

# configure task status consumer
taskStatusConsumerConfig:
  consumerClass: com.cognitree.kronos.queue.consumer.KafkaTaskStatusConsumer
  config:
    consumerConfig:
      bootstrap.servers : localhost:9092
      group.id: schedulerframework
      key.deserializer : org.apache.kafka.common.serialization.StringDeserializer
      value.deserializer : org.apache.kafka.common.serialization.StringDeserializer
    pollTimeout: 5s
    pollInterval: 5s
```

Here, [KafkaTaskProducer](src/main/java/com/cognitree/kronos/queue/producer/KafkaTaskProducer.java) is used as the producer of task to Kafka and [KafkaTaskConsumer](src/main/java/com/cognitree/kronos/queue/consumer/KafkaTaskConsumer.java) as consumer of task from Kafka. Similarly, [KafkaTaskStatusProducer](src/main/java/com/cognitree/kronos/queue/producer/KafkaTaskStatusProducer.java) is used as the producer of task status to Kafka and [KafkaTaskStatusConsumer](src/main/java/com/cognitree/kronos/queue/consumer/KafkaTaskStatusConsumer.java) as consumer of task status from Kafka.

Any property passed in `consumerConfig` or `producerConfig` section is passed as it while creating Consumer and Producer in Kafka.