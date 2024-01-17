package io.kontak.apps.temperature.generator;

import io.kontak.apps.event.TemperatureReading;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Component
public class SimpleTemperatureGenerator implements TemperatureGenerator { // TODO add REST generator

    private final Random random = new Random();

    private final List<String> thermometerIds = List.of(
            "thermometer-1",
            "thermometer-2",
            "thermometer-3"
    );

    private final Map<String, String> roomsByThermometer = Map.of(
            "thermometer-1", "room-A",
            "thermometer-2", "room-B",
            "thermometer-3", "room-B"
    );

    @Override
    public List<TemperatureReading> generate() {
        return List.of(generateSingleReading());
    }

    private TemperatureReading generateSingleReading() {
        //TODO basic implementation, should be changed to the one that will allow to test and demo solution on realistic data
        String thermometerId = thermometerIds.get(random.nextInt(3));
        return new TemperatureReading(
                random.nextDouble(10d, 30d),
                roomsByThermometer.get(thermometerId),
                thermometerId,
                Instant.now()
        );
    }
}
