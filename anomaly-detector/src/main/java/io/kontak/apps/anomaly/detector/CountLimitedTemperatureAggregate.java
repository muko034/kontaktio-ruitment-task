package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.TemperatureReading;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public record CountLimitedTemperatureAggregate(
        int maxCount,
        List<Double> previousTemperatures,
        AvgTemperatureAggregate avgTemperatureAggregate
) {

    public static CountLimitedTemperatureAggregate empty(int maxCount) {
        return new CountLimitedTemperatureAggregate(maxCount, new LinkedList<>(), AvgTemperatureAggregate.empty());
    }


    public CountLimitedTemperatureAggregate add(TemperatureReading temperatureReading) {
        var newTemperatures = new LinkedList<>(previousTemperatures);
        Optional<Double> lastTemperature = Optional.ofNullable(avgTemperatureAggregate.temperatureReading())
                .map(TemperatureReading::temperature);
        lastTemperature.ifPresent(newTemperatures::add);
        var newSum = avgTemperatureAggregate.sum() + lastTemperature.orElse(0.0);
        int newCount = avgTemperatureAggregate.count() + 1;
        if (avgTemperatureAggregate.count() == maxCount - 1) {
            var firstTemperature = newTemperatures.removeFirst();
            newSum -= firstTemperature;
            newCount -= 1;

        }
        return new CountLimitedTemperatureAggregate(maxCount, newTemperatures, new AvgTemperatureAggregate(temperatureReading, newSum, newCount));
    }

    public boolean isTemperatureHigherThenAverageBy(double threshold) {
        return avgTemperatureAggregate.isTemperatureHigherThenAverageBy(threshold);
    }

}
