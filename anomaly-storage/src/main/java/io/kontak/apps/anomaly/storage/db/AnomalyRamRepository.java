package io.kontak.apps.anomaly.storage.db;

import io.kontak.apps.anomaly.analyticsapi.AnomalyQueries;
import io.kontak.apps.anomaly.analyticsapi.ThermometerDto;
import io.kontak.apps.anomaly.storage.AnomalyRepository;
import io.kontak.apps.event.Anomaly;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

@Component
public class AnomalyRamRepository implements AnomalyRepository, AnomalyQueries { // TODO add real DB implementation e.g. MongoDB

    private final Collection<Anomaly> store = new ConcurrentLinkedQueue<>();

    @Override
    public Anomaly add(Anomaly anomaly) {
        store.add(anomaly);
        return anomaly;
    }

    @Override
    public List<Anomaly> listAnomaliesPerThermometer(String thermometerId) {
        return store.stream().filter(it -> it.thermometerId().equals(thermometerId)).toList();
    }

    @Override
    public List<Anomaly> listAnomaliesPerRoom(String roomId) {
        return store.stream().filter(it -> it.roomId().equals(roomId)).toList();
    }

    @Override
    public List<ThermometerDto> listThermometersWithAnomaliesMoreThan(int threshold) { // FIXME not optimal
        return store.stream().collect(groupingBy(Anomaly::thermometerId, counting()))
                .entrySet().stream().filter(it -> it.getValue() > threshold)
                .map(it -> new ThermometerDto(it.getKey(), it.getValue())).toList();
    }

}
