package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.TemperatureReading;

import java.io.Serializable;

public record TemperatureAggregate(TemperatureReading temperatureReading, double sum,
                                   int count) implements Serializable {

    private static final long serialVersionUID = 1L;

    public static TemperatureAggregate empty() {
        return new TemperatureAggregate(null, 0.0, 0);
    }

    public TemperatureAggregate add(TemperatureReading temperatureReading) {
        return new TemperatureAggregate(temperatureReading, sum + temperatureReading.temperature(), count + 1);
    }

    public double getAverage() {
        if (count == 0) {
            return 0.0; // Avoid division by zero
        }
        return sum / count;
    }

    public boolean isTemperatureHigherThenAverageBy(double threshold) {
        return temperatureReading != null && temperatureReading.temperature() > getAverage() + threshold;
    }

}
