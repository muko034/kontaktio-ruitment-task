package io.kontak.apps.anomaly.detector;

import io.kontak.apps.anomaly.detector.config.KafkaConfig;
import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class CountWindowAnomalyDetector implements AnomalyDetector {

    private static final Logger logger = LoggerFactory.getLogger(CountWindowAnomalyDetector.class);

    private final double temperatureThreshold;
    private final int countWindowLimit;

    public CountWindowAnomalyDetector(double temperatureThreshold, int countWindowLimit) {
        this.temperatureThreshold = temperatureThreshold;
        this.countWindowLimit = countWindowLimit;
    }

    @Override
    public KStream<String, Anomaly> apply(KStream<String, TemperatureReading> events) {
        SlidingWindows timeSlidingWindows = SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)); // FIXME dummy implementation based on arbitrary (long) time window
        KStream<String, SumCountTimestamp> sumCountTimestampStream = events
                .groupByKey(Grouped.with(KafkaConfig.stringSerde, KafkaConfig.temperatureReadingSerde))
                .windowedBy(timeSlidingWindows)
                .aggregate(
                        SumCountTimestamp::empty,
                        (key, temperatureReading, aggregate) -> {
                            if (aggregate != null && aggregate.count() < countWindowLimit) {
                                return aggregate.add(temperatureReading.temperature(), temperatureReading.timestamp());
                            } else {
                                return null;
                            }
                        },
                        Materialized.<String, SumCountTimestamp, WindowStore<Bytes, byte[]>>as(
                                        "count-window-temperature-aggregate-store"
                                )
                                .withKeySerde(KafkaConfig.stringSerde)
                                .withValueSerde(KafkaConfig.sumCountTimestampSerde)
                )
                .filter((key, aggregate) -> aggregate != null)
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value));
        return events
                .leftJoin(
                        sumCountTimestampStream,
                        (temperatureReading, sumCountTimestamp) -> {
                            if (sumCountTimestamp != null
                                    && !temperatureReading.timestamp().isAfter(sumCountTimestamp.timestamp())
                                    && temperatureReading.temperature() > sumCountTimestamp.calcAvg() + temperatureThreshold) {
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
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(timeSlidingWindows.timeDifferenceMs())),
                        StreamJoined.with(
                                KafkaConfig.stringSerde,
                                KafkaConfig.temperatureReadingSerde,
                                KafkaConfig.sumCountTimestampSerde
                        )
                )
                .transform(DeduplicateAnomalyTransformer::new, Named.as("count-window-anomaly-transformer"), KafkaConfig.STATE_STORE_NAME)
                .filter((key, anomaly) -> anomaly != null);
    }

}
