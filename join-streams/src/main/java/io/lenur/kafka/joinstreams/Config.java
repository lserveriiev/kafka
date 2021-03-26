package io.lenur.kafka.joinstreams;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class Config {
    @Value("${config.kafka_bootstrap_servers}")
    private String kafkaBootstrapServers;

    @Value("${config.kafka_schema_registry_url}")
    private String kafkaSchemaRegistryUrl;
}
