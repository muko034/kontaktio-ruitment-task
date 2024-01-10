package io.kontakt.apps.anomaly.detector;

import io.kontak.apps.anomaly.detector.AnomalyDetectorApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(classes = AnomalyDetectorApplication.class)
@Testcontainers
public class AbstractIntegrationTest {

    public final static KafkaContainer kafkaContainer;

    static {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
        kafkaContainer.start();
    }

    @DynamicPropertySource
    static void datasourceConfig(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.stream.binders.kafka.environment.spring.cloud.stream.kafka.streams.binder.brokers", kafkaContainer::getBootstrapServers);
    }

}
