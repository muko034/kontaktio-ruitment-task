package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

public class TimeWindowAnomalyDetector implements AnomalyDetector {

    private static final Logger logger = LoggerFactory.getLogger(TemperatureMeasurementsListener.class);

    private final double temperatureThreshold;
    private final Duration timeWindowDifference;
    private final Duration timeWindowGrace;

    public TimeWindowAnomalyDetector(double temperatureThreshold, Duration timeWindowDifference, Duration timeWindowGrace) {
        this.temperatureThreshold = temperatureThreshold;
        this.timeWindowDifference = timeWindowDifference;
        this.timeWindowGrace = timeWindowGrace;
    }

    @Override
    public KStream<String, Anomaly> apply(KStream<String, TemperatureReading> events) {
        JsonSerde<TemperatureReading> temperatureReadingJsonSerde = new JsonSerde<>(TemperatureReading.class);
        JsonSerde<AvgTemperatureAggregate> temperatureAggregateJsonSerde = new JsonSerde<>(AvgTemperatureAggregate.class);
        Serde<String> keySerde = Serdes.String();
        return events
                .groupByKey(Grouped.with(keySerde, temperatureReadingJsonSerde))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(timeWindowDifference, timeWindowGrace))
                .aggregate(
                        AvgTemperatureAggregate::empty,
                        (key, temperatureReading, avgTemperatureAggregate) -> avgTemperatureAggregate.add(temperatureReading),
                        Materialized.<String, AvgTemperatureAggregate, WindowStore<Bytes, byte[]>>as(
                                        "time-window-temperature-aggregate-store"
                                )
                                .withKeySerde(keySerde)
                                .withValueSerde(temperatureAggregateJsonSerde)
                )
                .filter((windowed, aggregate) -> aggregate.isTemperatureHigherThenAverageBy(temperatureThreshold))
                .mapValues((windowed, aggregate) -> {
                    TemperatureReading temperatureReading = aggregate.temperatureReading();
                    return new Anomaly(
                            temperatureReading.temperature(),
                            temperatureReading.roomId(),
                            temperatureReading.thermometerId(),
                            temperatureReading.timestamp()
                    );
                })
                .toStream()
                .map((windowed, anomaly) -> new KeyValue<>(windowed.key(), anomaly));
    }
}
