package io.kontak.apps.anomaly.detector;

import io.kontak.apps.event.Anomaly;
import io.kontak.apps.event.TemperatureReading;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public interface AnomalyDetector extends Function<KStream<String, TemperatureReading>, KStream<String, Anomaly>> {

}
