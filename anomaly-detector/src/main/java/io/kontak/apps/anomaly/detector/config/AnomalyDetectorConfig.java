package io.kontak.apps.anomaly.detector.config;

import io.kontak.apps.anomaly.detector.AlwaysAnomalyAnomalyDetector;
import io.kontak.apps.anomaly.detector.AnomalyDetector;
import io.kontak.apps.anomaly.detector.MeasurementsWindowAnomalyAnomalyDetector;
import io.kontak.apps.anomaly.detector.TimeWindowAnomalyAnomalyDetector;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AnomalyDetectorConfig {

    @Bean
    @ConditionalOnProperty(
            prefix = "io.kontak.apps.anomaly.detector",
            name = "algorithm",
            havingValue = "always"
    )
    public AnomalyDetector alwaysAnomalyAnomalyDetector() {
        return new AlwaysAnomalyAnomalyDetector();
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "io.kontak.apps.anomaly.detector",
            name = "algorithm",
            havingValue = "timeWindow"
    )
    public AnomalyDetector timeWindowAnomalyAnomalyDetector() {
        return new TimeWindowAnomalyAnomalyDetector();
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "io.kontak.apps.anomaly.detector",
            name = "algorithm",
            havingValue = "measurementsWindow"
    )
    public AnomalyDetector measurementsWindowAnomalyAnomalyDetector() {
        return new MeasurementsWindowAnomalyAnomalyDetector();
    }
}
