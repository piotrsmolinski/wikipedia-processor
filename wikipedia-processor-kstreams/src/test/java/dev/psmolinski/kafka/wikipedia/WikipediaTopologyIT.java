package dev.psmolinski.kafka.wikipedia;

import dev.psmolinski.kafka.wikipedia.model.WikiFeedMetric;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@SpringBootTest
@ActiveProfiles("embedded")
@ExtendWith(SpringExtension.class)
public class WikipediaTopologyIT {

    // These topics MUST be same as the topics defined in application-embedded.yml.
    // When embedded Kafka instance is created we do not have access to Spring populated configuration.
    private static final String INPUT_TOPIC = "wikipedia.parsed";
    private static final String OUTPUT_TOPIC = "wikipedia.parsed.count-by-domain";

    private static final int PARTITIONS = 4;

    private static EmbeddedKafkaBroker embeddedKafka;

    @BeforeAll
    public static void beforeAll() throws Exception {

        MockSchemaRegistry.dropScope("test");
        MockSchemaRegistry.getClientForScope("test")
                .register("wikipedia.parsed-value", readSchema("schema.03.avsc"), 1, 3);

        FileUtils.deleteDirectory(new File("target/kafka-streams"));

        embeddedKafka = new EmbeddedKafkaBroker(
                1,
                true,
                PARTITIONS,
                INPUT_TOPIC, OUTPUT_TOPIC
        )
        .brokerProperty("transaction.state.log.replication.factor", "1")
        .brokerProperty("transaction.state.log.min.isr", "1")
        .brokerProperty("auto.create.topics.enable", "false")
        ;

        embeddedKafka.afterPropertiesSet();

    }

    @AfterAll
    public static void afterAll() throws Exception {
        embeddedKafka.destroy();
    }

    // ---- tests ----

    @Test
    public void testTopics() throws Exception {

        streamsReady.get(60, TimeUnit.SECONDS);

        Map<String, List<PartitionInfo>> topics;
        try (Consumer<?,?> consumer = countsConsumerFactory.createConsumer()) {
            topics = consumer.listTopics();
        }

        System.out.println(topics.keySet());

    }

    @Test
    public void testSample() throws Exception {

        streamsReady.get(60, TimeUnit.SECONDS);

        BlockingQueue<WikiFeedMetric> results = new ArrayBlockingQueue<>(10);

        ContainerProperties properties = new ContainerProperties(
                IntStream.range(0, PARTITIONS)
                        .mapToObj(
                            p->new TopicPartitionOffset(
                                    OUTPUT_TOPIC,
                                    p,
                                    TopicPartitionOffset.SeekPosition.END)
                        )
                        .toArray(TopicPartitionOffset[]::new)
        );
        properties.setAckMode(ContainerProperties.AckMode.MANUAL);
        GenericMessageListenerContainer<String, WikiFeedMetric> container = new KafkaMessageListenerContainer<>(
                countsConsumerFactory,
                properties
        );
        container.setupMessageListener((MessageListener<String, WikiFeedMetric>) record -> results.add(record.value()));
        container.start();

        try {

            KafkaTemplate<String,byte[]> kafkaTemplate = new KafkaTemplate<>(editsProducerFactory);
            kafkaTemplate.setDefaultTopic(INPUT_TOPIC);

            WikiFeedMetric metric;

            kafkaTemplate
                    .sendDefault(
                            null,
                            readPayload("0-00000.avro")
                    )
                    .get();

            metric = results.poll(120, TimeUnit.SECONDS);

            Assertions.assertThat(metric)
                    .isNotNull();
            Assertions.assertThat(metric.getDomain())
                    .isEqualTo("www.wikidata.org");
            Assertions.assertThat(metric.getEditCount())
                    .isEqualTo(1L);
            Assertions.assertThat(results)
                    .isEmpty();

            kafkaTemplate
                    .sendDefault(
                            null,
                            readPayload("0-00000.avro")
                    )
                    .get();

            metric = results.poll(10, TimeUnit.SECONDS);

            Assertions.assertThat(metric)
                    .isNull();

        } finally {
            container.stop();
        }

    }


    // ---- wiring ----

    @Autowired
    private CompletableFuture<Void> streamsReady;

    @Autowired
    private ProducerFactory<String, byte[]> editsProducerFactory;

    @Autowired
    private ConsumerFactory<String, WikiFeedMetric> countsConsumerFactory;

    @TestConfiguration
    static class LocalTestConfiguration {

        @Autowired
        private KafkaProperties kafkaProperties;

        @Bean
        public CompletableFuture<Void> streamsReady() {
            return new CompletableFuture<>();
        }

        /**
         * We should not attempt to do anything against Kafka Streams instance until it is ready.
         * Normally one would use {@link org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer customizer},
         * but spring-kafka {@link org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration#defaultKafkaStreamsBuilder(ObjectProvider, ObjectProvider)
         * default instantiation} supports at most one instance and we have already one
         * in {@link WikipediaTopology#sbfCustomizer()}.
         * <br/>
         * Post-processors are executed on any bean available after they are present in the context.
         * The callbacks are fired before the target beans are exposed in the context.
         */
        @Bean
        public BeanPostProcessor sbfPostProcessor(CompletableFuture<Void> streamsReady) {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
                    if (bean instanceof StreamsBuilderFactoryBean) {
                        StreamsBuilderFactoryBean sbf = (StreamsBuilderFactoryBean) bean;
                        sbf.setStateListener(((newState, oldState) -> {
                            switch (newState) {
                                case RUNNING:
                                    streamsReady.complete(null);
                                    break;
                                case ERROR:
                                    streamsReady.completeExceptionally(new Exception("Kafka Streams app failed"));
                                    break;
                            }
                        }));
                    }
                    return null;
                }
            };
        }

        @Bean
        public ProducerFactory<String, byte[]> editsProducerFactory() {
            return new DefaultKafkaProducerFactory<>(
                    kafkaProperties.buildProducerProperties(),
                    new StringSerializer(),
                    new ByteArraySerializer()
            );
        }

        @Bean
        public ConsumerFactory<String, WikiFeedMetric> countsConsumerFactory() {
            Deserializer<WikiFeedMetric> deserializer = new SpecificAvroDeserializer<>();
            deserializer.configure(kafkaProperties.buildStreamsProperties(), false);
            return new DefaultKafkaConsumerFactory<>(
                    kafkaProperties.buildConsumerProperties(),
                    new StringDeserializer(),
                    deserializer
            );
        }

        /**
         * This bean definition is needed because {@link #suppliersMgmtProducerFactory}
         * prevents default instance from creation.
         * @see org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration#kafkaProducerFactory(ObjectProvider)
         */
        @Bean
        public ProducerFactory<?, ?> kafkaProducerFactory() {
            return new DefaultKafkaProducerFactory<>(
                    kafkaProperties.buildProducerProperties(),
                    new ByteArraySerializer(),
                    new ByteArraySerializer()
            );
        }

        /**
         * This bean definition is needed because {@link #suppliersMgmtConsumerFactory}
         * prevents default instance from creation.
         * @see org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration#kafkaConsumerFactory(ObjectProvider)
         */
        @Bean
        public ConsumerFactory<?,?> kafkaConsumerFactory() {
            return new DefaultKafkaConsumerFactory<>(
                    kafkaProperties.buildConsumerProperties(),
                    new ByteArrayDeserializer(),
                    new ByteArrayDeserializer()
            );
        }

    }

    private static AvroSchema readSchema(String schema) throws Exception {
        return new AvroSchema(FileUtils.readFileToString(
                new File("src/test/avro/"+schema),
                "utf-8"
        ));
    }

    private static byte[] readPayload(String message) throws Exception {
        return FileUtils.readFileToByteArray(new File("src/test/data/"+message));
    }

}
