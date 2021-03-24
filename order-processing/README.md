
# Exercise "Order Consumer" 

Use this link:

https://www.baeldung.com/spring-kafka

and update the OrderProcessorResource / OrderProcessorConfiguration,
to consume orders created from the kafka topic.

* first we just consume and log the orders, to get the feel of it.

Since the producer have the topic creation code, we can omit that for now.

Things you have to do for this excercise:

1. Change the kafka.inTopic (we use this first) setting in application.yml to something of your own (same as in order-producer...)
2. Set the kafka.groupId in application.yml to your own.
3. Set the server settings in the application.yml to point to the servers i provided.

If you have tested your order-producer, there should already be orders to 
consume, so when you fire it up for the first time it depends a bit how you 
configured your consumer factory, default is 'latest', but 'earliest' gives a
different behaviour:

https://kafka.apache.org/24/javadoc/constant-values.html#org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG

```
   props.put(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
          'earliest');
```

or 

```
   props.put(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
          'latest');
```

Once your application is started you can find your consumer group here:

```
$ k-groups --list 
```

And you can see which offset your consumer group is at here, if you have lag etc.

```
$ k-groups --describe --group <your group name> 
```

A test you can make is to:

1. Fire up your consumer for the first time, that way it will consume all messages or start at the end, and then command above will say lag = 0 for your consumer group.
2. Then shut down your consumer.
3. Then add an order with the order-producer.
4. Now the command above should say lag = 1, there is one message not consumed yet for that consumer group.
5. When you fire up your order-processor again, it will immediately consume the order and lag = 0 will be back...

   
# Exercise "Streaming Orders"

In this case lets create the topics by hand:

```
$ k-topic --create --topic <packTopic from application.yml> --partitions 1 --replication-factor 3
$ k-topic --create --topic <backorderTopic from application.yml> --partitions 1 --replication-factor 3
```

Now it is time to actually implement a streaming application. So we will simply
create a streaming application with a branch that actually splits our orders into 2 topics. 

Make your own inventive predicate on which orders to pack or send to backorder :-P.

Examples are sparse online unfortunately...

These are some useful links:

- We need to enable kafka streams processing in spring, and provide configuration for it:
  https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/annotation/EnableKafkaStreams.html
  and some hints n parameters, I would suggest setting at least 
  - APPLICATION_ID_CONFIG
  - BOOTSTRAP_SERVERS_CONFIG
  - ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> 'earliest', you want to start from the beginning always here.
  - DEFAULT_KEY_SERDE_CLASS_CONFIG, DEFAULT_VALUE_SERDE_CLASS_CONFIG, both to Serde.String().
  https://www.codota.com/web/assistant/code/rs/5c76b28096b4b300015b40e5#L82
    
- How to get a handle to a StreamsBuilder and configure it (spring boot will do the rest)
  Paragraph 14.3.3 is the structural thing to use here. And you will probably want to use the .to method in some way
  
  https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-kafka-streams

- The interface of the streamsbuilder to use. 

  https://kafka.apache.org/23/javadoc/org/apache/kafka/streams/StreamsBuilder.html

- A functional example using Predicates and the branch method on a KStream.
  https://github.com/spring-cloud/spring-cloud-stream-samples/blob/master/kafka-streams-samples/kafka-streams-branching/src/main/java/kafka/streams/branching/KafkaStreamsBranchingSample.java
  