package dev.psmolinski.kafka.wikipedia;

import dev.psmolinski.kafka.wikipedia.model.WikiFeedMetric;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.*;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.*;
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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Integration test example using embedded Kafka. In this test the Kafka Streams topology
 * and the test code run in the same JVM. Same applies to the underlying Kafka.
 * The main advantage is that it is possible to run this in IDE and set the breakpoints
 * inside topology for debugging. The state lifecycle is also limited to the single test run
 * as opposed to testing against shared Kafka instance. The test also verifies the component
 * wiring and lifecycle hooks.
 * <br/>
 * The downsides:
 * <ul>
 *     <li>Long startup and shutdown times</li>
 *     <li>Limited Kafka configuration options (like TLS)</li>
 *     <li>Asynchronous communication that makes absence detection long</li>
 * </ul>
 */
@SpringBootTest
// tell the Spring Boot to configure the application against in-process services
@ActiveProfiles("embedded")
@ExtendWith(SpringExtension.class)
// the rule allows us to launch the Kafka Broker
@EmbeddedKafka(
        partitions = 4,
        controlledShutdown = true,
        topics = {
                "wikipedia.parsed",
                "wikipedia.parsed.count-by-domain"
        },
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "auto.create.topics.enable=false"
        }
)
@DirtiesContext
public class WikipediaTopologyIT {

    // These topics MUST be same as the topics defined in application-embedded.yml.
    // When embedded Kafka instance is created we do not have access to Spring populated configuration.
    private static final String INPUT_TOPIC = "wikipedia.parsed";
    private static final String OUTPUT_TOPIC = "wikipedia.parsed.count-by-domain";

    @BeforeAll
    public static void beforeAll() throws Exception {

        MockSchemaRegistry.dropScope("test");
        MockSchemaRegistry.getClientForScope("test")
                .register("wikipedia.parsed-value", readSchema("schema.03.avsc"), 1, 3);

    }

    @BeforeEach
    public void beforeEach() throws Exception {
        streamsReady.get(60, TimeUnit.SECONDS);
    }

    // ---- tests ----

    /**
     * This is a good place to verify that the internal topics have deterministic names
     * and the number of partitions matching the input partitioning.
     */
    @Test
    public void testTopics() throws Exception {

        Map<String, List<PartitionInfo>> topics;
        try (Consumer<?,?> consumer = countsConsumerFactory.createConsumer()) {
            topics = consumer.listTopics();
        }

        SoftAssertions assertions = new SoftAssertions();
        // TreeMap is sorted by natural order of keys
        for (Map.Entry<String,List<PartitionInfo>> e : new TreeMap<>(topics).entrySet()) {
            // check only the internal topics, i.e. starting with <applicationId>-
            if (!e.getKey().startsWith("wikipedia-")) continue;
            // if we fail naming the nodes, we may see something like:
            // wikipedia-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition
            // such name is not deterministic as the project evolves
            assertions.assertThat(e.getKey())
                    .as("Topic %s, check for predefined name", e.getKey())
                    .doesNotContainPattern("\\d{10}");
            // verify that the created internal topics have same number of partitions
            // as the input
            assertions.assertThat(e.getValue())
                    .as("Topic %s, check for partition count", e.getKey())
                    .hasSize(4);
        }

        assertions.assertAll();
    }

    /**
     * The same test as usual to depict the technique and discuss its properties.
     * This test uses background consumption of all the messages from output topic.
     */
    @Test
    public void testSample() throws Exception {

        String correlationId = UUID.randomUUID().toString();

        BlockingQueue<WikiFeedMetric> results = new ArrayBlockingQueue<>(10);

        // Setup asynchronous consumer loop in form of container with callback.
        // The component is instantiated in the test because its lifecycle ends when the test
        // is completed.
        // Because the test could not be the only one in the suite it is a good idea
        // to use headers to select only relevant messages. Other tests in the same class
        // may leave messages in the same topic.
        // Please note that messages emitted with punctuator will not include triggering
        // message headers.
        GenericMessageListenerContainer<String, WikiFeedMetric> container =
                createMessageListenerContainer(
                        countsConsumerFactory,
                        OUTPUT_TOPIC,
                        record -> {
                            if (!matchesCorrelationId(record, correlationId)) return;
                            results.add(record.value());
                        }
                );
        KafkaTemplate<String,byte[]> template = new KafkaTemplate<>(editsProducerFactory);

        try {

            WikiFeedMetric metric;

            // send and wait for acknowledgement
            template.send(
                    createProducerRecord(
                            INPUT_TOPIC,
                            null,
                            readPayload("0-00000.avro"),
                            correlationId
                    )
            ).get();

            // wait for the first message; it should arrive quickly
            metric = results.poll(10, TimeUnit.SECONDS);

            Assertions.assertThat(metric)
                    .isNotNull();
            Assertions.assertThat(metric.getDomain())
                    .isEqualTo("www.wikidata.org");
            // this check is not the best one; there could be prior tests leading to changed value
            Assertions.assertThat(metric.getEditCount())
                    .isEqualTo(1L);
            Assertions.assertThat(results)
                    .isEmpty();

            template.send(
                    createProducerRecord(
                            INPUT_TOPIC,
                            null,
                            readPayload("0-00001.avro"),
                            correlationId
                    )
            ).get();

            // wait for any matching message; this is assertion for absence and should not be
            // included in such tests
            metric = results.poll(10, TimeUnit.SECONDS);

            Assertions.assertThat(metric)
                    .isNull();

        } finally {
            template.destroy();
            container.stop();
        }

    }

    /**
     * Boilerplate code to create the background consumer loop.
     * @param consumerFactory
     * @param topic
     * @param callback
     * @param <K>
     * @param <V>
     * @return
     */
    private <K,V> GenericMessageListenerContainer<K, V> createMessageListenerContainer(
            ConsumerFactory<K, V> consumerFactory,
            String topic,
            java.util.function.Consumer<ConsumerRecord<K,V>> callback) {

        // Read partitions
        List<PartitionInfo> partitions;
        try (Consumer<?,?> consumer = consumerFactory.createConsumer()) {
            partitions = consumer.partitionsFor(topic);
        }

        // Setup asynchronous consumer loop in form of container with callback.
        // The component is instantiated in the test because its lifecycle ends when the test
        // is completed.
        ContainerProperties properties = new ContainerProperties(
                partitions.stream()
                        .map(p->new TopicPartitionOffset(p.topic(),p.partition(), TopicPartitionOffset.SeekPosition.END))
                        .toArray(TopicPartitionOffset[]::new)
        );
        properties.setAckMode(ContainerProperties.AckMode.MANUAL);

        GenericMessageListenerContainer<K, V> container = new KafkaMessageListenerContainer<>(
                consumerFactory,
                properties
        );
        container.setupMessageListener((MessageListener<K, V>) callback::accept);
        container.start();

        return container;

    }

    private <K,V> ProducerRecord<K,V> createProducerRecord(String topic, K key, V value, String correlationId) {
        ProducerRecord<K,V> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.ISO_8859_1));
        return record;
    }

    private <K,V> boolean matchesCorrelationId(ConsumerRecord<K,V> record, String correlationId) {
        Objects.requireNonNull(record);
        Objects.requireNonNull(correlationId);
        if (record.headers()==null) return false;
        Header header = record.headers().lastHeader("correlationId");
        if (header==null) return false;
        return correlationId.equals(new String(header.value(), StandardCharsets.ISO_8859_1));
    }

    // ---- wiring ----

    /**
     * The component can be used in the {@link #beforeAll()} to perform further topic setup.
     */
    @Autowired
    private EmbeddedKafkaBroker kafkaEmbedded;

    @Autowired
    private CompletableFuture<Void> streamsReady;

    @Autowired
    private ProducerFactory<String, byte[]> editsProducerFactory;

    @Autowired
    private ConsumerFactory<String, WikiFeedMetric> countsConsumerFactory;

    /**
     * Test specific wiring including Kafka Streams topology readiness check and components
     * used by the test itself.
     */
    @TestConfiguration
    static class LocalTestConfiguration {

        @Autowired
        private KafkaProperties kafkaProperties;

        @Bean
        public CleanupConfig cleanupConfig() {
            return new CleanupConfig(true, true);
        }

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

    // ---- utilities ----

    /**
     * Read Avro schema in Schema Registry format to preregister it with id matching the one
     * captured in the test messages.
     * @param schema
     * @return
     */
    private static AvroSchema readSchema(String schema) throws Exception {
        return new AvroSchema(FileUtils.readFileToString(
                new File("src/test/avro/"+schema),
                "utf-8"
        ));
    }

    /**
     * Read the test message as raw payload. The messages contain the schema id that must be
     * upfront registered in the Schema Registry, otherwise the receiver would not know hot to
     * parse them.
     * @param message
     * @return
     */
    private static byte[] readPayload(String message) throws Exception {
        return FileUtils.readFileToByteArray(new File("src/test/data/"+message));
    }

}
