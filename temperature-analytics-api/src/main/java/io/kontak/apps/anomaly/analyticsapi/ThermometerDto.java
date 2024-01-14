package io.kontak.apps.anomaly.analyticsapi;

public record ThermometerDto(
        String thermometerId,
        long anomaliesCount
) {
}
