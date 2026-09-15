package com.jamespolk.ordertracking.events;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.time.Duration;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The registry, not the code, is the gate on schema changes. With BACKWARD compatibility a new
 * optional field passes and a new required field is rejected.
 */
class SchemaRegistryCompatibilityTest {

    static final Network network = Network.newNetwork();
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1")
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");
    static final GenericContainer<?> registry = new GenericContainer<>("confluentinc/cp-schema-registry:8.3.1")
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)))
            .dependsOn(kafka);

    static SchemaRegistryClient client;

    @BeforeAll
    static void start() {
        registry.start();
        client =
                new CachedSchemaRegistryClient("http://" + registry.getHost() + ":" + registry.getMappedPort(8081), 10);
    }

    @AfterAll
    static void stop() {
        registry.stop();
        kafka.stop();
    }

    @Test
    void backwardCompatibilityAcceptsOptionalFieldAndRejectsRequiredField() throws Exception {
        String subject = "order-events-" + OrderPlaced.class.getName();
        client.register(subject, new AvroSchema(OrderPlaced.getClassSchema()));
        client.updateCompatibility(subject, "BACKWARD");

        Schema v2 = new Schema.Parser().parse(getClass().getResourceAsStream("/avro/OrderPlaced_v2.avsc"));
        Schema v3 = new Schema.Parser().parse(getClass().getResourceAsStream("/avro/OrderPlaced_v3_breaking.avsc"));

        assertThat(client.testCompatibility(subject, new AvroSchema(v2))).isTrue();
        assertThat(client.testCompatibility(subject, new AvroSchema(v3))).isFalse();
    }
}
