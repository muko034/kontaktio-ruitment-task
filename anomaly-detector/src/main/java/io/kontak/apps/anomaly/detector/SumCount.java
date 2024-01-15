package io.kontak.apps.anomaly.detector;

public record SumCount(
        double sum,
        int count
) {

    public static SumCount empty() {
        return new SumCount(0.0, 0);
    }

    public SumCount add(double value) {
        return new SumCount(sum + value, count + 1);
    }

    public double calcAvg() {
        if (count > 0) {
            return sum / count;
        }
        return 0.0;
    }
}
