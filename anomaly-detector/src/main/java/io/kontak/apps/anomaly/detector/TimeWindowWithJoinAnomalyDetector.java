package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class TimeWindowWithJoinAnomalyDetector implements AnomalyDetector {

    private static final Logger logger = LoggerFactory.getLogger(TemperatureMeasurementsListener.class);

    private final double temperatureThreshold;
    private final Duration timeWindowDifference;
    private final Duration timeWindowGrace;

    public TimeWindowWithJoinAnomalyDetector(double temperatureThreshold, Duration timeWindowDifference, Duration timeWindowGrace) {
        this.temperatureThreshold = temperatureThreshold;
        this.timeWindowDifference = timeWindowDifference;
        this.timeWindowGrace = timeWindowGrace;
    }

    @Override
    public KStream<String, Anomaly> apply(KStream<String, TemperatureReading> events) {
        JsonSerde<TemperatureReading> temperatureReadingJsonSerde = new JsonSerde<>(TemperatureReading.class);
        JsonSerde<SumCount> temperatureAggregateJsonSerde = new JsonSerde<>(SumCount.class);
        JsonSerde<Anomaly> anomalyJsonSerde = new JsonSerde<>(Anomaly.class);
        JsonSerde<AnomalyAggregate> anomalyAggregateJsonSerde = new JsonSerde<>(AnomalyAggregate.class);
        Serde<String> keySerde = Serdes.String();
        SlidingWindows timeSlidingWindows = SlidingWindows.ofTimeDifferenceAndGrace(timeWindowDifference, timeWindowGrace);
        Serde<Double> averageSerde = Serdes.Double();
        return events
                .groupByKey(Grouped.with(keySerde, temperatureReadingJsonSerde))
                .windowedBy(timeSlidingWindows)
                .aggregate(
                        SumCount::empty,
                        (key, temperatureReading, aggregate) -> aggregate.add(temperatureReading.temperature()),
                        Materialized.<String, SumCount, WindowStore<Bytes, byte[]>>as(
                                        "time-window-temperature-aggregate-store"
                                )
                                .withKeySerde(keySerde)
                                .withValueSerde(temperatureAggregateJsonSerde)
                )
                .mapValues((key, aggregate) -> aggregate.sum() / aggregate.count(),
                        Named.as("time-window-temperature-average-store"),
                        Materialized.with(
                                WindowedSerdes.timeWindowedSerdeFrom(
                                        String.class,
                                        timeSlidingWindows.timeDifferenceMs()
                                ),
                                averageSerde)
                )
                .suppress(Suppressed.untilWindowCloses(unbounded()).withName("average-time-window-suppressed"))
                .toStream()
                .filter((windowed, avg) -> avg != null)
                .map((windowed, avg) -> new KeyValue<>(windowed.key(), avg))
                .peek((windowed, avg) -> logger.info(String.format("Key: %s, Avg: %s", windowed, avg)))
                .join(
                        events,
                        (averageTemperature, temperatureReading) -> {
                            if (temperatureReading.temperature() > averageTemperature + temperatureThreshold) {
                                return new Anomaly(
                                        temperatureReading.temperature(),
                                        temperatureReading.roomId(),
                                        temperatureReading.thermometerId(),
                                        temperatureReading.timestamp()
                                );
                            } else {
                                return null;
                            }
                        },
                        JoinWindows.ofTimeDifferenceAndGrace(timeWindowDifference, timeWindowGrace),
                        StreamJoined.with(
                                keySerde,
                                averageSerde,
                                temperatureReadingJsonSerde)
                )
                .filter((key, anomaly) -> anomaly != null)
                .groupByKey(Grouped.with(keySerde, anomalyJsonSerde))
                .windowedBy(timeSlidingWindows)
                .aggregate(
                        AnomalyAggregate::empty,
                        (key, anomaly, aggregate) -> aggregate.add(anomaly),
                        Named.as("time-window-anomaly-aggregate-store"),
                        Materialized.with(keySerde, anomalyAggregateJsonSerde)
                )
                .suppress(Suppressed.untilWindowCloses(unbounded()).withName("time-window-anomaly-aggregate-suppressed"))
                .toStream()
                .flatMap((windowed, aggregate) ->
                        aggregate.anomalies().stream().map(it-> KeyValue.pair(windowed.key(), it)).toList()
                ); // FIXME
    }
}
