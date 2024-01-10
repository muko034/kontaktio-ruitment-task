package io.kontak.apps.temperature.generator;

import io.kontak.apps.event.TemperatureReading;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Component
public class SimpleTemperatureGenerator implements TemperatureGenerator {

    private final Random random = new Random();

    @Override
    public List<TemperatureReading> generate() {
        return List.of(generateSingleReading());
    }

    private TemperatureReading generateSingleReading() {
        //TODO basic implementation, should be changed to the one that will allow to test and demo solution on realistic data
        return new TemperatureReading(
                random.nextDouble(10d, 30d),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                Instant.now()
        );
    }
}
