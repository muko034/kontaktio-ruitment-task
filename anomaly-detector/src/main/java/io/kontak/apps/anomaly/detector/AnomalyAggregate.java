package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;

import java.util.HashSet;
import java.util.Set;

public record AnomalyAggregate(
        Set<Anomaly> anomalies
) {
    public static AnomalyAggregate empty() {
        return new AnomalyAggregate(new HashSet<>());
    }

    public AnomalyAggregate add(Anomaly anomaly) {
        var newAnomalies = new HashSet<>(anomalies);
        newAnomalies.add(anomaly);
        return new AnomalyAggregate(newAnomalies);
    }
}
