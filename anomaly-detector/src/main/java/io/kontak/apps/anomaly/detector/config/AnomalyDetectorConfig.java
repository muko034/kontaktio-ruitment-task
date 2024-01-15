package io.kontak.apps.anomaly.detector.config;

import io.kontak.apps.anomaly.detector.AlwaysAnomalyAnomalyDetector;
import io.kontak.apps.anomaly.detector.AnomalyDetector;
import io.kontak.apps.anomaly.detector.CountWindowAnomalyDetector;
import io.kontak.apps.anomaly.detector.TimeWindowAnomalyDetector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class AnomalyDetectorConfig {

    public static final String DEFAULT_TEMPERATURE_THRESHOLD = "5.0";

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
    public AnomalyDetector timeWindowAnomalyAnomalyDetector(
            @Value("${io.kontak.apps.anomaly.detector.temperatureThreshold:" + DEFAULT_TEMPERATURE_THRESHOLD + "}")
            double temperatureThreshold,
            @Value("${io.kontak.apps.anomaly.detector.timeWindowDifference:10s}")
            Duration timeWindowDifference,
            @Value("${io.kontak.apps.anomaly.detector.timeWindowGrace:500ms}")
            Duration timeWindowGrace
    ) {
        return new TimeWindowAnomalyDetector(temperatureThreshold, timeWindowDifference, timeWindowGrace);
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "io.kontak.apps.anomaly.detector",
            name = "algorithm",
            havingValue = "countWindow"
    )
    public AnomalyDetector countWindowAnomalyAnomalyDetector(
            @Value("${io.kontak.apps.anomaly.detector.temperatureThreshold:" + DEFAULT_TEMPERATURE_THRESHOLD + "}")
            double temperatureThreshold,
            @Value("${io.kontak.apps.anomaly.detector.countWindowLimit:10}")
            int countWindowLimit
    ) {
        return new CountWindowAnomalyDetector(temperatureThreshold, countWindowLimit);
    }
}
