package io.kontakt.apps.anomaly.detector;

import io.kontak.apps.anomaly.detector.AnomalyDetector;
import io.kontak.apps.anomaly.detector.TimeWindowAnomalyDetector;
import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeWindowAnomalyDetectorTest extends AbstractAnomalyDetectorTest {

    @Override
    protected AnomalyDetector createAnomalyDetector() {
        return new TimeWindowAnomalyDetector(
                5.0,
                Duration.ofSeconds(10),
                Duration.ofMillis(500)
        );
    }

    @Test
    public void shouldAggregateRecords() {
        //given
        final String room1 = "room-1";
        final String thermometer1 = "thermometer-1";
        final String thermometer2 = "thermometer-2";
        final Instant instant = Instant.now();
        final List<TemperatureReading> temperatureReadings = List.of(
                new TemperatureReading(10.0, room1, thermometer1, instant),
                new TemperatureReading(11.0, room1, thermometer1, instant.plusSeconds(3)),
                new TemperatureReading(20.0, room1, thermometer1, instant.plusSeconds(6)),
                new TemperatureReading(9.0, room1, thermometer1, instant.plusSeconds(9)),
                new TemperatureReading(-20.0, room1, thermometer2, instant.plusSeconds(12)),
                new TemperatureReading(15.0, room1, thermometer1, instant.plusSeconds(15))
        );

        //when
        temperatureReadings.forEach(it -> inputTopic.pipeInput(it.thermometerId(), it));
        List<Anomaly> actualValues = outputTopic.readValuesToList();

        //then
        List<Anomaly> expectedValues = List.of(
                new Anomaly(20.0, room1, thermometer1, instant.plusSeconds(6))
        );
        assertEquals(expectedValues, actualValues);

    }
}
