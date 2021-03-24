package se.cag.labs.order.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import se.cag.labs.kafka.model.Order;

/**
 * Service that in
 * Iteration 1: Listens to your order kafka topic and prints the orders on stdout...
 * Iteration 2: Streams Kafka topic and sends orders to 2 distinct Sink:s.
 */
@Service
@Slf4j
public class OrderProcessorService {

    private OrderProcessorConfiguration configuration;
    private ObjectMapper mapper = new ObjectMapper();

    public OrderProcessorService(OrderProcessorConfiguration configuration) {
        this.configuration = configuration;
    }

    @Bean
    public KStream<Object,Object>[] kStream(StreamsBuilder streamsBuilder) {
        KStream<Object, Object>[] branches = streamsBuilder.stream(configuration.getInTopic())
                .branch((k, v) -> inStock((String)v), (k, v) -> ! inStock((String) v));
        branches[0].to(configuration.getPackTopic());
        branches[1].to(configuration.getBackorderTopic());
        return branches;
    }

    private boolean inStock(String value) {
        try {
            Order order = mapper.readValue(value, Order.class);
            return order.getLines().stream().allMatch(l -> l.getSku().startsWith("I"));
        } catch (JsonProcessingException e) {
            return false;
        }
    }

}
