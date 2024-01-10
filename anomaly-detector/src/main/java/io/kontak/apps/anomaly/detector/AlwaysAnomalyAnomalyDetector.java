package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
public class AlwaysAnomalyAnomalyDetector implements AnomalyDetector {
    @Override
    public Optional<Anomaly> apply(List<TemperatureReading> temperatureReadings) {

        TemperatureReading temperatureReading = temperatureReadings.get(0);
        return Optional.of(new Anomaly(
                temperatureReading.temperature(),
                temperatureReading.roomId(),
                temperatureReading.thermometerId(),
                temperatureReading.timestamp()
        ));
    }
}
