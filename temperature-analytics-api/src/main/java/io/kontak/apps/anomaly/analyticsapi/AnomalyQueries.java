package io.kontak.apps.anomaly.analyticsapi;

import io.kontak.apps.event.Anomaly;

import java.util.List;

public interface AnomalyQueries {

    List<Anomaly> listAnomaliesPerThermometer(String thermometerId);

    List<Anomaly> listAnomaliesPerRoom(String roomId);

    List<ThermometerDto> listThermometersWithAnomaliesMoreThan(int threshold);

}
