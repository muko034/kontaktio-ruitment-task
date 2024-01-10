package io.kontak.apps.temperature.generator.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kontak.apps.event.TemperatureReading;
import io.kontak.apps.temperature.generator.TemperatureStreamPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

import java.util.TimeZone;
import java.util.function.Supplier;

@Configuration
public class KafkaConfig {

    @Bean
    public Supplier<Flux<Message<TemperatureReading>>> messageProducer(TemperatureStreamPublisher publisher) {
        return publisher::getMessageProducer;
    }

    @Bean
    public static ObjectMapper objectMapper() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true)
                .defaultDateFormat(new StdDateFormat().withTimeZone(TimeZone.getTimeZone("UTC")).withColonInTimeZone(false))
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }


}
