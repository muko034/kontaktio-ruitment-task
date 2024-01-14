package io.kontak.apps.anomaly.storage;

import io.kontak.apps.event.Anomaly;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class AnomalyListener implements Consumer<KStream<String, Anomaly>> {

    private static final Logger logger = LoggerFactory.getLogger(AnomalyListener.class);

    @Override
    public void accept(KStream<String, Anomaly> events) {
        events.foreach((key, anomaly) -> logger.info(String.format("Anomaly: %s, Key: %s", anomaly, key)));
    }

}
