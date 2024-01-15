package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;

public class CountWindowAnomalyDetector implements AnomalyDetector {

    private final double temperatureThreshold;
    private final  int countWindowLimit;

    public CountWindowAnomalyDetector(double temperatureThreshold, int countWindowLimit) {
        this.temperatureThreshold = temperatureThreshold;
        this.countWindowLimit = countWindowLimit;
    }

    @Override
    public KStream<String, Anomaly> apply(KStream<String, TemperatureReading> events) {
        JsonSerde<TemperatureReading> temperatureReadingJsonSerde = new JsonSerde<>(TemperatureReading.class);
        JsonSerde<CountLimitedTemperatureAggregate> temperatureAggregateJsonSerde = new JsonSerde<>(CountLimitedTemperatureAggregate.class);
        Serde<String> keySerde = Serdes.String();
        return events
                .groupByKey(Grouped.with(keySerde, temperatureReadingJsonSerde))
                .aggregate(
                        () -> CountLimitedTemperatureAggregate.empty(countWindowLimit),
                        (key, temperatureReading, aggregate) -> aggregate.add(temperatureReading),
                        Materialized.<String, CountLimitedTemperatureAggregate, KeyValueStore<Bytes, byte[]>>as(
                                        "count-window-temperature-aggregate-store"
                                )
                                .withKeySerde(keySerde)
                                .withValueSerde(temperatureAggregateJsonSerde)
                )
                .filter((key, aggregate) -> aggregate.isTemperatureHigherThenAverageBy(temperatureThreshold))
                .mapValues((key, aggregate) -> {
                    TemperatureReading temperatureReading = aggregate.avgTemperatureAggregate().temperatureReading();
                    return new Anomaly(
                            temperatureReading.temperature(),
                            temperatureReading.roomId(),
                            temperatureReading.thermometerId(),
                            temperatureReading.timestamp()
                    );
                })
                .toStream();
    }

}
