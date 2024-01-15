package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.TemperatureReading;

import java.util.LinkedList;
import java.util.List;

public record AvgTemperatureAggregate2(
        double sum,
        int count,

        double average,
        List<TemperatureReading> readings
) {

    public static AvgTemperatureAggregate2 empty() {
        return new AvgTemperatureAggregate2(0.0, 0, 0.0, new LinkedList<>());
    }

    public AvgTemperatureAggregate2 add(TemperatureReading temperatureReading) {
        List<TemperatureReading> newPrevTemp = new LinkedList<>(readings);
        newPrevTemp.add(temperatureReading);
        double newSum = sum + temperatureReading.temperature();
        return new AvgTemperatureAggregate2(newSum, newPrevTemp.size(), newSum / newPrevTemp.size(), newPrevTemp);
    }

    public double getAverage() {
        if (count == 0) {
            return 0.0;
        }
        return sum / count;
    }

    public boolean anyTemperatureHigherThenAverageBy(double threshold) {
        return readings.stream().mapToDouble(TemperatureReading::temperature).anyMatch(it -> it > getAverage() + threshold) ;
    }

    public List<TemperatureReading> getTemperaturesHigherThenAverageBy(double threshold) {
        return readings.stream().filter(it -> it.temperature() > getAverage() + threshold).toList();
    }

}
