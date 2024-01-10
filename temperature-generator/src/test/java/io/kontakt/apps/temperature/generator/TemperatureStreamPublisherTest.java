package io.kontakt.apps.temperature.generator;

import io.kontak.apps.event.TemperatureReading;
import io.kontak.apps.temperature.generator.TemperatureStreamPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.time.Instant;

public class TemperatureStreamPublisherTest extends AbstractIntegrationTest {

    @Autowired
    private TemperatureStreamPublisher publisher;

    @Value("${spring.cloud.stream.bindings.messageProducer-out-0.destination}")
    private String topic;

    @Test
    void testRecordPublishing() {

        try (TestKafkaConsumer<TemperatureReading> consumer = new TestKafkaConsumer<>(
                kafkaContainer.getBootstrapServers(),
                topic,
                TemperatureReading.class
        )) {
            TemperatureReading temperatureReading = new TemperatureReading(20d, "room", "thermometer", Instant.parse("2023-01-01T00:00:00.000Z"));
            publisher.publish(temperatureReading);
            consumer.drain(
                    consumerRecords -> consumerRecords.stream().anyMatch(r -> r.value().thermometerId().equals(temperatureReading.thermometerId())),
                    Duration.ofSeconds(5)
            );
        }
    }
}
