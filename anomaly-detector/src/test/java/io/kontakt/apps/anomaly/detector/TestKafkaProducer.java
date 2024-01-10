package io.kontakt.apps.anomaly.detector;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class TestKafkaProducer<T> implements Closeable {
    private final Producer<String, T> producer;
    private final String topic;

    public TestKafkaProducer(String bootstrapServer, String topic) {
        this.producer = configureProducer(bootstrapServer);
        this.topic = topic;
    }

    public void produce(String key, T value) {
        this.producer.send(new ProducerRecord<>(
                topic,
                key,
                value
        ));
        this.producer.flush();
    }

    private Producer<String, T> configureProducer(String bootstrapServers) {
        Map<String, Object> producerProperties = new HashMap<>(KafkaTestUtils.producerProps(bootstrapServers));
        return new DefaultKafkaProducerFactory<String, T>(producerProperties,
                new StringSerializer(), new JsonSerializer<>()).createProducer();
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
