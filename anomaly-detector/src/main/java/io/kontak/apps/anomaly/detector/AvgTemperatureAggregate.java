package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.TemperatureReading;

public record AvgTemperatureAggregate(
        TemperatureReading temperatureReading,
        double sum,
        int count
) {

    public static AvgTemperatureAggregate empty() {
        return new AvgTemperatureAggregate(null, 0.0, 0);
    }

    public AvgTemperatureAggregate add(TemperatureReading temperatureReading) {
        return new AvgTemperatureAggregate(temperatureReading, sum + temperatureReading.temperature(), count + 1);
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
