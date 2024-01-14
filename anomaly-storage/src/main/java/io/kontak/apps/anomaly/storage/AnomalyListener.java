package io.kontak.apps.anomaly.storage;

import io.kontak.apps.event.Anomaly;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class AnomalyListener implements Consumer<KStream<String, Anomaly>> {

    private static final Logger logger = LoggerFactory.getLogger(AnomalyListener.class);
    private final AnomalyRepository anomalyRepository;

    public AnomalyListener(AnomalyRepository anomalyRepository) {
        this.anomalyRepository = anomalyRepository;
    }

    @Override
    public void accept(KStream<String, Anomaly> events) {
        events
                .peek((key, anomaly) -> logger.info(String.format("Anomaly: %s, Key: %s", anomaly, key)))
                .filter((key, anomaly) -> anomaly != null)
                .foreach((key, anomaly) -> anomalyRepository.add(anomaly));
    }

}
