package se.cag.labs.order.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import se.cag.labs.kafka.model.Order;

@RestController
@Slf4j
public class OrderProducerResource {

    private OrderProducerConfiguration configuration;
    private KafkaTemplate<String, String> template;

    private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public OrderProducerResource(OrderProducerConfiguration configuration,
                                 KafkaTemplate<String, String> template) {
        this.configuration = configuration;
        this.template = template;
    }

    @PostMapping("orders/order")
    public void produceOrder(@RequestBody Order order) {
        log.info(order.toString());
        try {
            String orderAsString = mapper.writeValueAsString(order);
            template.send(configuration.getTopic(), orderAsString);
        } catch (JsonProcessingException e) {
            log.error("Could not serialize order");
        }
        // Just dump order on kafka for processing.
    }

}
