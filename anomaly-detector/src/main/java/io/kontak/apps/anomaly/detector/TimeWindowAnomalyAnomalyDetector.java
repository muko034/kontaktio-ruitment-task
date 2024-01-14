package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

public class TimeWindowAnomalyAnomalyDetector implements AnomalyDetector {

    private static final Logger logger = LoggerFactory.getLogger(TemperatureMeasurementsListener.class);

    @Override
    public KStream<String, Anomaly> apply(KStream<String, TemperatureReading> events) {
        JsonSerde<TemperatureReading> temperatureReadingJsonSerde = new JsonSerde<>(TemperatureReading.class);
        JsonSerde<TemperatureAggregate> temperatureAggregateJsonSerde = new JsonSerde<>(TemperatureAggregate.class);
        Serde<String> keySerde = Serdes.String();
        return events
                .map((k, v) -> new KeyValue<>(v.thermometerId(), v)) // FIXME For some reason key is null
//                .peek((key, temperatureReading) -> logger.info(String.format("Key: %s, Temperature reading: %s", key, temperatureReading)))
                .groupByKey(Grouped.with(keySerde, temperatureReadingJsonSerde))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofMillis(500)))
                .aggregate(
                        TemperatureAggregate::empty,
                        (key, temperatureReading, temperatureAggregate) -> temperatureAggregate.add(temperatureReading),
                        Materialized.with(keySerde, temperatureAggregateJsonSerde)
                )
                .filter((windowed, aggregate) -> aggregate.isTemperatureHigherThenAverageBy(5.0))
                .mapValues((s, aggregate) -> {
                    TemperatureReading temperatureReading = aggregate.temperatureReading();
                    return new Anomaly(
                            temperatureReading.temperature(),
                            temperatureReading.roomId(),
                            temperatureReading.thermometerId(),
                            temperatureReading.timestamp()
                    );
                })
                .toStream().map((k, v) -> new KeyValue<>(k.key(), v));
    }
}
