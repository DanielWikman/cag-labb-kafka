
alias k-config-topic="/opt/kafka/bin/kafka-configs.sh --zookeeper ${ZOOKEEPER} --entity-type topics --entity-name "
alias k-topic="/opt/kafka/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER} "
alias k-consumer="/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA} "
alias k-groups="/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server ${KAFKA} "
alias k-producer="/opt/kafka/bin/kafka-console-producer.sh --broker-list ${KAFKA} --topic "

# tail:                  k-consumer -topic <>
# less:                  k-consumer --from-beginning -topic <>
# list consumers:        k-groups --list
# reset consumer group:  k-groups --reset-offsets <to where> --group <> --topic <> --execute
# current group offset:  k-groups --describe --new-consumer --group <>
