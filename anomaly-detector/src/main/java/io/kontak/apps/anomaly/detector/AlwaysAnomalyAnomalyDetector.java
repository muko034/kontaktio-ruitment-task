package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlwaysAnomalyAnomalyDetector implements AnomalyDetector {

    private static final Logger logger = LoggerFactory.getLogger(TemperatureMeasurementsListener.class);

    @Override
    public KStream<String, Anomaly> apply(KStream<String, TemperatureReading> events) {
        return events
//                .peek((key, temperatureReading) -> logger.info(String.format("Temperature reading: %s", temperatureReading)))
                .mapValues(temperatureReading -> new Anomaly(
                        temperatureReading.temperature(),
                        temperatureReading.roomId(),
                        temperatureReading.thermometerId(),
                        temperatureReading.timestamp()
                ));
    }

}
