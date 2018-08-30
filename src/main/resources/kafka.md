# Kafka

## Observations

### Using Apache Kafka programmatically

[Spring offers][Spring Kafka] a usual range of high to low level access objects.
* Both, sending and receiving of messages is pretty straightforward.
* (De)serializing of structured data [using JSON][Storing JSON] is also awailable.
* [Using Kafka as a Data Store][Using Kafka as a Data Store] is possible.
* [Storing larger files][Larger files] (> 5MB) takes some effort or workarounds.

### Clients
There are several commandline tools and also a few UIs for both, managing and viewing Kafka:
* Kafka Manager: <https://github.com/yahoo/kafka-manager>
* Landoop Kafka Topics: <https://github.com/Landoop/kafka-topics-ui>
* Kafka Tool: <http://www.kafkatool.com/>
* Kafka Commandline Tools: <https://www.cloudera.com/documentation/kafka/latest/topics/kafka_command_line.html>

## Resources

* [Kafka Quickstart]: <http://kafka.apache.org/quickstart>
* [Spring Kafka]: <https://docs.spring.io/spring-kafka/reference/htmlsingle/>
* [Storing JSON]: <https://www.codenotfound.com/spring-kafka-json-serializer-deserializer-example.html>
* [Using Kafka as a Data Store]: <https://www.confluent.io/blog/okay-store-data-apache-kafka/>
* [Larger files]: <https://www.quora.com/Is-it-a-bad-practice-to-put-large-files-into-Kafka>


