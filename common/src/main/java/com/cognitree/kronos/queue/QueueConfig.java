package com.cognitree.kronos.queue;

import com.cognitree.kronos.queue.consumer.ConsumerConfig;
import com.cognitree.kronos.queue.producer.ProducerConfig;

import java.util.Objects;

/**
 * defines configuration required by the application to create producer and consumer to exchange message between
 * scheduler and executor.
 */
public class QueueConfig {
    private ProducerConfig producerConfig;
    private ConsumerConfig consumerConfig;
    private String taskStatusQueue;

    public ProducerConfig getProducerConfig() {
        return producerConfig;
    }

    public void setProducerConfig(ProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
    }

    public ConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    public void setConsumerConfig(ConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public String getTaskStatusQueue() {
        return taskStatusQueue;
    }

    public void setTaskStatusQueue(String taskStatusQueue) {
        this.taskStatusQueue = taskStatusQueue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueConfig)) return false;
        QueueConfig that = (QueueConfig) o;
        return Objects.equals(producerConfig, that.producerConfig) &&
                Objects.equals(consumerConfig, that.consumerConfig) &&
                Objects.equals(taskStatusQueue, that.taskStatusQueue);
    }

    @Override
    public int hashCode() {

        return Objects.hash(producerConfig, consumerConfig, taskStatusQueue);
    }

    @Override
    public String toString() {
        return "QueueConfig{" +
                "producerConfig=" + producerConfig +
                ", consumerConfig=" + consumerConfig +
                ", taskStatusQueue='" + taskStatusQueue + '\'' +
                '}';
    }
}
