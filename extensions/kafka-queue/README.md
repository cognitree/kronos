# Kafka Queue

Kafka Queue is an extension for queue which uses [Kafka](http://kafka.apache.org) to exchange to message between scheduler and executor.

## Configuring Kafka Queue

Update the `queue.yaml` to use Kafka as queue
```
producerConfig:
  producerClass: com.cognitree.kronos.queue.producer.KafkaProducerImpl
  config:
    kafkaProducerConfig:
      bootstrap.servers : localhost:9092
      key.serializer : org.apache.kafka.common.serialization.StringSerializer
      value.serializer : org.apache.kafka.common.serialization.StringSerializer
consumerConfig:
  consumerClass: com.cognitree.kronos.queue.consumer.KafkaConsumerImpl
  config:
    kafkaConsumerConfig:
      bootstrap.servers : localhost:9092
      group.id: kronos
      key.deserializer : org.apache.kafka.common.serialization.StringDeserializer
      value.deserializer : org.apache.kafka.common.serialization.StringDeserializer
    pollTimeout: 5s
  pollInterval: 5s
taskStatusQueue: taskstatus
```

Here, [KafkaTaskProducer](src/main/java/com/cognitree/kronos/queue/producer/KafkaTaskProducer.java) is used as the producer of task to Kafka and [KafkaTaskConsumer](src/main/java/com/cognitree/kronos/queue/consumer/KafkaTaskConsumer.java) as consumer of task from Kafka. Similarly, [KafkaTaskStatusProducer](src/main/java/com/cognitree/kronos/queue/producer/KafkaTaskStatusProducer.java) is used as the producer of task status to Kafka and [KafkaTaskStatusConsumer](src/main/java/com/cognitree/kronos/queue/consumer/KafkaTaskStatusConsumer.java) as consumer of task status from Kafka.

Any property passed in `kafkaConsumerConfig` or `kafkaProducerConfig` section is passed as it while creating Consumer and Producer in Kafka.
