package dev.psmolinski.kafka.wikipedia;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class WikipediaApplication {

    public static void main(String...args) {
        SpringApplication.run(WikipediaApplication.class, args);
    }

}
