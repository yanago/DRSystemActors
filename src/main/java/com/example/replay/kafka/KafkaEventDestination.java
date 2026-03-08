package com.example.replay.kafka;

import com.example.replay.api.EventDestination;
import com.example.replay.util.JsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Sends events to Kafka with customer-based partition key so that all events
 * for the same customer (cid) go to the same partition (heavy customers use keys consistently).
 */
public final class KafkaEventDestination implements EventDestination {

    public static final String TOPIC_KEY = "kafka_topic";
    public static final String BOOTSTRAP_SERVERS_KEY = "kafka_bootstrap_servers";
    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaEventDestination(String topic, String bootstrapServers) {
        this.topic = Objects.requireNonNull(topic, "topic");
        String servers = bootstrapServers != null && !bootstrapServers.isBlank() ? bootstrapServers : DEFAULT_BOOTSTRAP;
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
    }

    public static KafkaEventDestination fromConfig(Map<String, Object> config) {
        Object topicObj = config.get(TOPIC_KEY);
        String topic = topicObj != null ? topicObj.toString() : "replay-events";
        Object serversObj = config.get(BOOTSTRAP_SERVERS_KEY);
        String servers = serversObj != null ? serversObj.toString() : null;
        return new KafkaEventDestination(topic, servers);
    }

    @Override
    public void sendBatch(List<Object> records) {
        if (records == null) return;
        for (Object record : records) {
            String key = CustomerPartitionKey.keyFor(record);
            String value = JsonUtil.toJson(record);
            producer.send(new ProducerRecord<>(topic, key, value));
        }
        producer.flush();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
