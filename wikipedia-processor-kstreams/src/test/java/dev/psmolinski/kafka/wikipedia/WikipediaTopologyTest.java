package dev.psmolinski.kafka.wikipedia;

import dev.psmolinski.kafka.wikipedia.model.WikiFeedMetric;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test case with {@link TopologyTestDriver}. The test is Spring Boot independent, although it is
 * possible to leverage some configuration classes.
 * <br/>
 * The test focuses on the Kafka Streams topology itself. The huge advantage is that all the
 * processing in the topology is done on explicit trigger and the control does not return
 * to the caller until the action is fully completed. This includes also message routing
 * through a topic (for example for repartitioning), which normally results in two consecutive
 * graph processing.
 * <br/>
 * The disadvantage is that the code is not executed by the regular Kafka Streams orchestration.
 * Therefore we test something executed using different codebase than the normal app.
 *
 */
public class WikipediaTopologyTest {

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, byte[]> inputTopic;
    private TestOutputTopic<String, WikiFeedMetric> outputTopic;

    @BeforeEach
    public void beforeEach() throws Exception {

        MockSchemaRegistry.dropScope("test");
        MockSchemaRegistry.getClientForScope("test")
                .register("wikipedia.parsed-value", readSchema("schema.03.avsc"), 1, 3);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        WikipediaProperties wikipediaProperties = WikipediaProperties.builder()
                .topics(
                        WikipediaProperties.Topics.builder()
                                .input("wikipedia.parsed")
                                .output("wikipedia.parsed.count-by-domain")
                                .build()
                )
                .build();

        WikipediaTopology topology = new WikipediaTopology(wikipediaProperties);

        topology.buildTopology(streamsBuilder);

        // do the cleanup
        FileUtils.deleteDirectory(new File("target/test/streams"));

        Topology kstreamTopology = streamsBuilder.build();
        // it is good to review the topology graph
        System.out.println(kstreamTopology.describe().toString());

        testDriver = new TopologyTestDriver(
                kstreamTopology,
                PropertiesBuilder.newBuilder()
                        .with(StreamsConfig.STATE_DIR_CONFIG, "target/test/streams")
                        .with(StreamsConfig.APPLICATION_ID_CONFIG, "test")
                        .with(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                        .with(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName())
                        .with(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName())
                        .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test")
                        .build()
        );

        inputTopic = testDriver.createInputTopic(
                wikipediaProperties.getTopics().getInput(),
                new StringSerializer(),
                new ByteArraySerializer()
        );

        Deserializer<WikiFeedMetric> deserializer = new SpecificAvroDeserializer<>();
        deserializer.configure(MapBuilder.newBuilder(String.class, Object.class)
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test")
                .build(), false);

        outputTopic = testDriver.createOutputTopic(
                wikipediaProperties.getTopics().getOutput(),
                new StringDeserializer(),
                deserializer
        );

    }

    @AfterEach
    public void afterEach() throws Exception {
        if (testDriver!=null) {
            testDriver.close();
            testDriver = null;
        }
    }

    // ---- tests ----

    @Test
    public void testSimple() throws Exception {

        // first edit is for www.wikidata.org
        inputTopic.pipeInput(null, readPayload("0-00000.avro"));

        Assertions.assertThat(outputTopic.getQueueSize())
                .isEqualTo(1);

        WikiFeedMetric metric = outputTopic.readValue();

        Assertions.assertThat(metric.getDomain())
                .isEqualTo("www.wikidata.org");
        Assertions.assertThat(metric.getEditCount())
                .isEqualTo(1L);

        // second edit is reported as bot
        inputTopic.pipeInput(null, readPayload("0-00001.avro"));

        // the test for absence can be executed immediately, no new output messages will be
        // sent until we call inputTopic.pipeInput or testDriver.advanceWallClockTime
        Assertions.assertThat(outputTopic.getQueueSize())
                .isEqualTo(0);

    }

    // ---- utilities ----

    private AvroSchema readSchema(String schema) throws Exception {
        return new AvroSchema(FileUtils.readFileToString(
                new File("src/test/avro/"+schema),
                "utf-8"
        ));
    }

    private byte[] readPayload(String message) throws Exception {
        return FileUtils.readFileToByteArray(new File("src/test/data/"+message));
    }

    /**
     * Utility to create configuration using fluent API.
     */
    private static class PropertiesBuilder {
        private Properties properties = new Properties();
        public static PropertiesBuilder newBuilder() {
            return new PropertiesBuilder();
        }
        public PropertiesBuilder with(String k, String v) {
            properties.put(k, v);
            return this;
        }
        public Properties build() {
            return properties;
        }
    }

    /**
     * Utility to create configuration maps using fluent API.
     */
    private static class MapBuilder<K,V> {
        private Map<K,V> map = new HashMap<>();
        public static <K,V> MapBuilder<K,V> newBuilder(Class<K> k, Class<V> v) {
            return new MapBuilder<>();
        }
        public MapBuilder<K,V> with(K k, V v) {
            map.put(k, v);
            return this;
        }
        public Map<K,V> build() {
            return map;
        }
    }


}
