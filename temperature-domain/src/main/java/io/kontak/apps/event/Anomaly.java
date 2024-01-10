package io.kontak.apps.event;

import java.time.Instant;

public record Anomaly(double temperature, String roomId, String thermometerId, Instant timestamp) {
}
