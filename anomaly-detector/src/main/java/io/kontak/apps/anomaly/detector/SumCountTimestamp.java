package io.kontak.apps.anomaly.detector;

import java.time.Instant;

public record SumCountTimestamp(
        double sum,
        int count,

        Instant timestamp
) {

    public static SumCountTimestamp empty() {
        return new SumCountTimestamp(0.0, 0, null);
    }

    public SumCountTimestamp add(double value, Instant timestamp) {
        return new SumCountTimestamp(sum + value, count + 1, timestamp);
    }

    public double calcAvg() {
        if (count > 0) {
            return sum / count;
        }
        return 0.0;
    }
}
