package io.kontak.apps.anomaly.detector;

import io.kontak.apps.anomaly.detector.config.KafkaConfig;
import io.kontak.apps.event.Anomaly;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

class DeduplicateAnomalyTransformer implements Transformer<String, Anomaly, KeyValue<String, Anomaly>> {
    private KeyValueStore<Anomaly, Boolean> anomalyStore;

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        anomalyStore = context.getStateStore(KafkaConfig.STATE_STORE_NAME);
    }

    @Override
    public KeyValue<String, Anomaly> transform(String key, Anomaly anomaly) {
        // Check if anomaly has already been detected for this key
        if (anomaly != null && anomalyStore.get(anomaly) == null) {
            anomalyStore.put(anomaly, true);
            return new KeyValue<>(key, anomaly);
        }
        return null; // Skip the anomaly if already detected
    }

    @Override
    public void close() {
        // No additional cleanup needed
    }
}
