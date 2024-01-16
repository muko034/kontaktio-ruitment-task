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
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        SlidingWindows timeSlidingWindows = SlidingWindows.ofTimeDifferenceAndGrace(timeWindowDifference, timeWindowGrace);
        KStream<String, Double> averageTemperatures = events
                .groupByKey(Grouped.with(KafkaConfig.stringSerde, KafkaConfig.temperatureReadingSerde))
                .windowedBy(timeSlidingWindows)
                .aggregate(
                        SumCount::empty,
                        (key, temperatureReading, aggregate) -> aggregate.add(temperatureReading.temperature()),
                        Materialized.<String, SumCount, WindowStore<Bytes, byte[]>>as(
                                        "time-window-temperature-aggregate-store"
                                )
                                .withKeySerde(KafkaConfig.stringSerde)
                                .withValueSerde(KafkaConfig.sumCountSerde)
                )
                .mapValues((key, aggregate) -> aggregate.calcAvg(),
                        Named.as("time-window-temperature-average-store"),
                        Materialized.with(
                                WindowedSerdes.timeWindowedSerdeFrom(
                                        String.class,
                                        timeSlidingWindows.timeDifferenceMs()
                                ),
                                KafkaConfig.doubleSerde)
                )
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value));
        return events
                .leftJoin(
                        averageTemperatures,
                        (temperatureReading, averageTemperature) -> {
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
                                KafkaConfig.stringSerde,
                                KafkaConfig.temperatureReadingSerde,
                                KafkaConfig.doubleSerde
                        )
                )
                .transform(AnomalyTransformer::new, Named.as("anomaly-transformer"), KafkaConfig.STATE_STORE_NAME)
                .filter((key, anomaly) -> anomaly != null);
    }
}

class AnomalyTransformer implements Transformer<String, Anomaly, KeyValue<String, Anomaly>> {
    private KeyValueStore<Anomaly, String> anomalyStore;

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        anomalyStore = context.getStateStore(KafkaConfig.STATE_STORE_NAME);
    }

    @Override
    public KeyValue<String, Anomaly> transform(String key, Anomaly anomaly) {
        // Check if anomaly has already been detected for this key
        if (anomaly != null && anomalyStore.get(anomaly) == null) {
            anomalyStore.put(anomaly, "true");
            return new KeyValue<>(key, anomaly);
        }
        return null; // Skip the anomaly if already detected
    }

    @Override
    public void close() {
        // No additional cleanup needed
    }
}
