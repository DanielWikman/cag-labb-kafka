package se.cag.labs.kafka.stock;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class StockServiceGlobalStoreService implements KafkaStreams.StateListener {

    private StreamsBuilderFactoryBean factoryBean;
    private StockServiceConfiguration configuration;
    private ReadOnlyKeyValueStore<String, String> stocks;

    @Autowired
    public StockServiceGlobalStoreService(StreamsBuilderFactoryBean factoryBean, StockServiceConfiguration configuration) {
        this.factoryBean = factoryBean;
        this.configuration = configuration;
        this.factoryBean.setStateListener(this);
    }

    @Override
    public void onChange(KafkaStreams.State state, KafkaStreams.State state1) {
        if (state == KafkaStreams.State.RUNNING) {
            this.stocks = factoryBean.getKafkaStreams().store(configuration.getGlobalStore(), QueryableStoreTypes.keyValueStore());
        }
    }

    public ReadOnlyKeyValueStore<String, String> getStockMap() {
        return stocks;
    }
}
