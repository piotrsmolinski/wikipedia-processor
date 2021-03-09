# Wikipedia Processor in Spring Boot

The project provides Spring Boot reimplementation of KStreams application from cp-demo.

https://github.com/confluentinc/cp-demo/tree/6.1.0-post/kstreams-app/

The original implementation is a very nice application to observe Kafka Streams under load.
The problem is how to build similar application, but leveraging dependency injection
and configuration features coming from IoC framework like Spring Boot.

This project rewires the original topology and provides examples how to write
unit tests with KafkaTopologyDriver and integration tests with embedded Kafka.
