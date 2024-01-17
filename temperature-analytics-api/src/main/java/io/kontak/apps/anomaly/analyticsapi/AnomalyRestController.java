package io.kontak.apps.anomaly.analyticsapi;

import io.kontak.apps.event.Anomaly;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1")
public class AnomalyRestController {

    private final AnomalyQueries anomalyQueries;

    public AnomalyRestController(AnomalyQueries anomalyQueries) {
        this.anomalyQueries = anomalyQueries;
    }

    @GetMapping("/thermometers/{thermometerId}/anomalies") // TODO Add pagination
    public List<Anomaly> listAnomaliesPerThermometer(@PathVariable String thermometerId) {
        return anomalyQueries.listAnomaliesPerThermometer(thermometerId);
    }

    @GetMapping("/rooms/{roomId}/anomalies")
    public List<Anomaly> listAnomaliesPerRoom(@PathVariable String roomId) {
        return anomalyQueries.listAnomaliesPerRoom(roomId);
    }

    @GetMapping("/thermometers")
    public List<ThermometerDto> getAnomaliesPerThermometer(@RequestParam int moreAnomaliesThan) {
        return anomalyQueries.listThermometersWithAnomaliesMoreThan(moreAnomaliesThan);
    }

}
