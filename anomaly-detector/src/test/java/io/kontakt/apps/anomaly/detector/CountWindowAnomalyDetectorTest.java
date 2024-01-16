package io.kontakt.apps.anomaly.detector;

import io.kontak.apps.anomaly.detector.AnomalyDetector;
import io.kontak.apps.anomaly.detector.CountWindowAnomalyDetector;
import io.kontak.apps.anomaly.detector.TimeWindowAnomalyDetector;
import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CountWindowAnomalyDetectorTest extends AbstractAnomalyDetectorTest {

    @Override
    protected AnomalyDetector createAnomalyDetector() {
        return new CountWindowAnomalyDetector(
                5.0,
                10
        );
    }

    @Test
    public void shouldDetectAnomalies() {
        //given
        final String room1 = "room-1";
        final String thermometer1 = "thermometer-1";
        final Instant instant = Instant.now();
        final List<TemperatureReading> temperatureReadings = List.of(
                new TemperatureReading(20.1, room1, thermometer1, instant),
                new TemperatureReading(21.2, room1, thermometer1, instant.plusSeconds(1)),
                new TemperatureReading(20.3, room1, thermometer1, instant.plusSeconds(2)),
                new TemperatureReading(19.1, room1, thermometer1, instant.plusSeconds(3)),
                new TemperatureReading(20.1, room1, thermometer1, instant.plusSeconds(4)),
                new TemperatureReading(19.2, room1, thermometer1, instant.plusSeconds(5)),
                new TemperatureReading(20.1, room1, thermometer1, instant.plusSeconds(6)),
                new TemperatureReading(18.1, room1, thermometer1, instant.plusSeconds(7)),
                new TemperatureReading(19.4, room1, thermometer1, instant.plusSeconds(8)),
                new TemperatureReading(20.1, room1, thermometer1, instant.plusSeconds(11)),
                new TemperatureReading(27.1, room1, thermometer1, instant.plusSeconds(12)),
                new TemperatureReading(23.1, room1, thermometer1, instant.plusSeconds(13))
        );

        //when
        temperatureReadings.forEach(it -> inputTopic.pipeInput(it.thermometerId(), it, it.timestamp()));
        List<Anomaly> actualValues = outputTopic.readValuesToList();

        //then
        List<Anomaly> expectedValues = List.of(
                new Anomaly(27.1, room1, thermometer1, instant.plusSeconds(12))
        );
        assertEquals(expectedValues, actualValues);

    }
}
