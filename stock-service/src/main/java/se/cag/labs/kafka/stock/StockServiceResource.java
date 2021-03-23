package se.cag.labs.kafka.stock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import se.cag.labs.kafka.model.Stock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RestController
@Slf4j
public class StockServiceResource {

    private StockServiceConfiguration configuration;
    private StockServiceGlobalStoreService stockGlobalStore;
    private ObjectMapper mapper = new ObjectMapper();

    public StockServiceResource(StockServiceConfiguration configuration, StockServiceGlobalStoreService stockGlobalStore) {
        this.configuration = configuration;
        this.stockGlobalStore = stockGlobalStore;
    }

    @GetMapping("stocks/{sku}")
    public Stock getStock(@PathVariable String sku) throws IOException {
        String value = stockGlobalStore.getStockMap().get(sku);
        if (value != null) {
            return mapper.readValue(value.getBytes(StandardCharsets.UTF_8), Stock.class);
        }
        log.info(sku);
        return null;
    }

    @GetMapping("stocks")
    public List<Stock> getStock() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(stockGlobalStore.getStockMap().all(), -1), false)
                .map(this::toStock)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Stock toStock(KeyValue<String, String> stringStringKeyValue) {
        try {
            return mapper.readValue(stringStringKeyValue.value, Stock.class);
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

}
