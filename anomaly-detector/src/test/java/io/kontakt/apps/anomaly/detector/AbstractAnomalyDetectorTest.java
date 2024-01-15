package io.kontakt.apps.anomaly.detector;

import io.kontak.apps.anomaly.detector.AnomalyDetector;
import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

public abstract class AbstractAnomalyDetectorTest {

    final String inputTopicName = "input";
    final String outputTopicName = "output";

    final Serde<String> stringSerde = Serdes.String();
    final JsonSerde<TemperatureReading> temperatureReadingSerde = new JsonSerde<>(TemperatureReading.class);
    final JsonSerde<Anomaly> anomalySerde = new JsonSerde<>(Anomaly.class);

    Properties streamsProps;
    KStream<String, TemperatureReading> temperatureReadingStream;
    StreamsBuilder streamsBuilder;

    TopologyTestDriver testDriver;

    protected TestInputTopic<String, TemperatureReading> inputTopic;

    protected TestOutputTopic<String, Anomaly> outputTopic;

    AnomalyDetector anomalyDetector;

    @BeforeEach
    public void setup() {
        anomalyDetector = createAnomalyDetector();

        // Properties
        streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test");
        streamsProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        streamsProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        streamsProps.put("schema.registry.url", "mock://aggregation-test");

        // Topology
        streamsBuilder = new StreamsBuilder();

        temperatureReadingStream =
                streamsBuilder.stream(inputTopicName, Consumed.with(stringSerde, temperatureReadingSerde));

        anomalyDetector.apply(temperatureReadingStream)
                .to(outputTopicName, Produced.with(Serdes.String(), anomalySerde));

        testDriver = new TopologyTestDriver(streamsBuilder.build(), streamsProps);

        inputTopic = testDriver.createInputTopic(
                inputTopicName,
                stringSerde.serializer(),
                temperatureReadingSerde.serializer()
        );

        outputTopic = testDriver.createOutputTopic(
                outputTopicName,
                stringSerde.deserializer(),
                anomalySerde.deserializer()
        );
    }

    @AfterEach
    public void cleanup() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    protected abstract AnomalyDetector createAnomalyDetector();

}
