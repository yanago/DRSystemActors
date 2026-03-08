package com.example.replay.api;

import com.example.replay.kafka.KafkaEventDestination;
import com.example.replay.rest.RestEventDestination;
import com.example.replay.rest.SimulatedRestDestination;

import java.util.Map;

/**
 * Creates EventDestination from job parameters (destination type, Kafka topic, REST URL, etc.).
 * <p>
 * Configuration (job parameters):
 * <ul>
 *   <li>{@value #DESTINATION_KEY}: "kafka" or "rest" (default "rest")</li>
 *   <li>Kafka: {@value com.example.replay.kafka.KafkaEventDestination#TOPIC_KEY}, {@value com.example.replay.kafka.KafkaEventDestination#BOOTSTRAP_SERVERS_KEY} (default localhost:9092). Partition key = cid (heavy customers use keys consistently).</li>
 *   <li>REST: {@value com.example.replay.rest.RestEventDestination#REST_URL_KEY}. POST JSON to downstream API. If blank or "http://simulate" uses in-memory simulation.</li>
 * </ul>
 */
public final class EventDestinationFactory {

    public static final String DESTINATION_KEY = "destination";
    public static final String DESTINATION_KAFKA = "kafka";
    public static final String DESTINATION_REST = "rest";

    private EventDestinationFactory() {
    }

    /**
     * Returns a destination based on config. "destination" = "kafka" | "rest" (default "rest").
     * For Kafka: kafka_topic, kafka_bootstrap_servers. For REST: rest_url (if blank or "http://simulate" returns simulated in-memory destination).
     */
    public static EventDestination create(Map<String, Object> config) {
        if (config == null) config = Map.of();
        String dest = config.containsKey(DESTINATION_KEY)
                ? String.valueOf(config.get(DESTINATION_KEY)).toLowerCase().trim()
                : DESTINATION_REST;
        if (DESTINATION_KAFKA.equals(dest)) {
            return KafkaEventDestination.fromConfig(config);
        }
        Object urlObj = config.get(RestEventDestination.REST_URL_KEY);
        String url = urlObj != null ? urlObj.toString() : "";
        if (url.isBlank() || SimulatedRestDestination.isSimulationUrl(url)) {
            return new SimulatedRestDestination();
        }
        return RestEventDestination.fromConfig(config);
    }
}
