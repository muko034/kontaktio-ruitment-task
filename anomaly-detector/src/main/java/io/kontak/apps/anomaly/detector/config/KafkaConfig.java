package io.kontak.apps.anomaly.detector.config;

import io.kontak.apps.anomaly.detector.AnomalyDetector;
import io.kontak.apps.anomaly.detector.SumCount;
import io.kontak.apps.anomaly.detector.TemperatureMeasurementsListener;
import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.function.Function;

@Configuration
public class KafkaConfig {

    public static final JsonSerde<Anomaly> anomalySerde = new JsonSerde<>(Anomaly.class);
    public static final JsonSerde<TemperatureReading> temperatureReadingSerde = new JsonSerde<>(TemperatureReading.class);
    public static final JsonSerde<SumCount> sumCountSerde = new JsonSerde<>(SumCount.class);
    public static final Serde<String> stringSerde = Serdes.String();
    public static final Serde<Double> doubleSerde = Serdes.Double();
    public static final String STATE_STORE_NAME = "anomaly-store";

    @Bean
    public Function<KStream<String, TemperatureReading>, KStream<String, Anomaly>> anomalyDetectorProcessor(AnomalyDetector anomalyDetector) {
        return new TemperatureMeasurementsListener(anomalyDetector);
    }

    @Bean
    public StoreBuilder anomalyStore() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME), anomalySerde, Serdes.String());
    }

}
