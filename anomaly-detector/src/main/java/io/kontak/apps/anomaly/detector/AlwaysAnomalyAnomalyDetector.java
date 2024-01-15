package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.streams.kstream.KStream;

public class AlwaysAnomalyAnomalyDetector implements AnomalyDetector {

    @Override
    public KStream<String, Anomaly> apply(KStream<String, TemperatureReading> events) {
        return events
                .mapValues(temperatureReading -> new Anomaly(
                        temperatureReading.temperature(),
                        temperatureReading.roomId(),
                        temperatureReading.thermometerId(),
                        temperatureReading.timestamp()
                ));
    }

}
