package dev.psmolinski.kafka.wikipedia;

import dev.psmolinski.kafka.wikipedia.model.WikiFeedMetric;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;

@Configuration
@EnableConfigurationProperties(WikipediaProperties.class)
public class WikipediaTopology {

    private final static Logger log = LoggerFactory.getLogger(WikipediaTopology.class);

    private final WikipediaProperties wikipediaProperties;
    private final KafkaProperties kafkaProperties;

    public WikipediaTopology(WikipediaProperties wikipediaProperties, KafkaProperties kafkaProperties) {
        this.wikipediaProperties = wikipediaProperties;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Create callback hook to generate topology on the default {@link org.springframework.kafka.config.StreamsBuilderFactoryBean factory}.
     * Note that the customizer is called before the factory is exposed in Spring, even before post-processors
     * are called.
     * @return
     */
    @Bean
    public StreamsBuilderFactoryBeanCustomizer sbfCustomizer() {
        return sbf-> sbf.setInfrastructureCustomizer(
                    new KafkaStreamsInfrastructureCustomizer() {
                        @Override
                        public void configureBuilder(StreamsBuilder builder) {
                            buildTopology(builder);
                        }
                    });
    }

    public void buildTopology(StreamsBuilder streamsBuilder) {

        streamsBuilder.<String, GenericRecord>stream(wikipediaProperties.getTopics().getInput(), Consumed.as("input"))
                // INPUT_TOPIC has no key so use domain as the key
                .map((key, value) -> new KeyValue<>(((GenericRecord)value.get("meta")).get("domain").toString(), value))
                .filter((key, value) -> !(boolean)value.get("bot"))
                .groupByKey(Grouped.as("by-domain"))
                .count(Materialized.as("counts"))
                .mapValues(WikiFeedMetric::new)
                .toStream()
                .peek((key, value) -> log.debug("{}:{}", key, value.getEditCount()))
                .to(wikipediaProperties.getTopics().getOutput(), Produced.as("output"));

    }

}
