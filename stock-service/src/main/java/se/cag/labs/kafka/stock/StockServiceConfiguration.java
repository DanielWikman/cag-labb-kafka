package se.cag.labs.kafka.stock;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka")
@Data
public class StockServiceConfiguration {
    private String servers;
    private String topic;
    private int replication;
    private int partitions;
    private String application;
    private String globalStore;
}
