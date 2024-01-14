package io.kontak.apps.anomaly.storage.config;

import io.kontak.apps.anomaly.analyticsapi.config.AnalyticsApiConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(AnalyticsApiConfig.class)
public class AnomalyStorageConfig {
}
