package io.kontak.apps.anomaly.storage;

import io.kontak.apps.event.Anomaly;

public interface AnomalyRepository {

    Anomaly add(Anomaly anomaly);
}
