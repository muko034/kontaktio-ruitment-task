package io.kontak.apps.anomaly.detector.config;

import io.kontak.apps.anomaly.detector.AlwaysAnomalyAnomalyDetector;
import io.kontak.apps.anomaly.detector.AnomalyDetector;
import io.kontak.apps.anomaly.detector.CountWindowAnomalyDetector;
import io.kontak.apps.anomaly.detector.TimeWindowAnomalyDetector;
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
        return new TimeWindowAnomalyDetector();
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "io.kontak.apps.anomaly.detector",
            name = "algorithm",
            havingValue = "countWindow"
    )
    public AnomalyDetector countWindowAnomalyAnomalyDetector() {
        return new CountWindowAnomalyDetector();
    }
}
