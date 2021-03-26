package se.cag.labs.order.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import se.cag.labs.kafka.model.Order;
import se.cag.labs.kafka.model.OrderLine;
import se.cag.labs.kafka.model.Stock;

import javax.validation.constraints.Null;

/**
 * Service that in
 * Iteration 1: Listens to your order kafka topic and prints the orders on stdout...
 * Iteration 2: Streams Kafka topic and sends orders to 2 distinct Sink:s.
 */
@Service
@Slf4j
public class OrderProcessorService implements KafkaStreams.StateListener {

    private OrderProcessorConfiguration configuration;
    private StreamsBuilderFactoryBean factoryBean;
    private ObjectMapper mapper = new ObjectMapper();
    private ReadOnlyKeyValueStore<Object, Object> stocks;

    public OrderProcessorService(StreamsBuilderFactoryBean factoryBean, OrderProcessorConfiguration configuration) {
        this.configuration = configuration;
        this.factoryBean = factoryBean;
        factoryBean.setStateListener(this);
    }

    @Bean
    public KStream<String,String>[] kStream(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .globalTable("stocks-dawi", Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("stock"));
        KStream<String, String>[] branches = streamsBuilder.stream(configuration.getInTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .branch((k, v) -> inStock(v), (k, v) -> ! inStock(v));
        branches[0].to(configuration.getPackTopic());
        branches[1].to(configuration.getBackorderTopic());
        return branches;
    }

    private boolean inStock(String value) {
        try {
            Order order = mapper.readValue(value, Order.class);
            return order.getLines().stream().allMatch(this::inGlobalTable);
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    private boolean inGlobalTable(OrderLine l) {
        try {
            Stock stock = mapper.readValue((String) stocks.get(l.getSku()), Stock.class);
            return stock.getStock() > 0;
        } catch (JsonProcessingException | NullPointerException e ) {
            return false;
        }
    }

    @Override
    public void onChange(KafkaStreams.State state, KafkaStreams.State state1) {
        if (state.equals(KafkaStreams.State.RUNNING)) {
            this.stocks = factoryBean.getKafkaStreams().store("stock", QueryableStoreTypes.keyValueStore());
        }
    }
}
