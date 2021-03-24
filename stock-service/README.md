
# Exercise "Stock Service - Prepping Producer" 

So first, I have prepared the producer, so you should first just go into the stock-producer/resources/application.yml
and setup the IP addresses of the servers, and your own topic name, as usual.

Then, you need to add the actual producing code, this topic should be keyed based on sku.

Fire it up to get the topic created.

# Exercise "Stock Service - Making a superfast service."

Use links:

https://www.confluent.io/blog/spring-for-apache-kafka-deep-dive-part-2-apache-kafka-spring-cloud-stream/
https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/annotation/EnableKafkaStreams.html
https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-kafka-streams
https://spring.io/blog/2019/12/09/stream-processing-with-spring-cloud-stream-and-apache-kafka-streams-part-6-state-stores-and-interactive-queries

and update the StockServiceConfiguration and StockServiceResource,
to use a GlobalKTable / ReadOnlyKeyValueStore from the kafka topic to serve stocks.

This link contains some hints on which attributes you need to set on the StreamsConfig:
https://stackoverflow.com/questions/51733039/kafka-streams-with-spring-boot

Since the stock-producer have the topic creation code, we can omit that for now.

Things you have to do for this excercise:

1. again, private settings in application.yml
2. Set the server settings in the application.yml to point to the servers i provided.

