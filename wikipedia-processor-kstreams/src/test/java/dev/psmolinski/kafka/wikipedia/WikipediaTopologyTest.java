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
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

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

        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.getStreams().setStateDir("target/test/streams");
        kafkaProperties.getStreams().setApplicationId("test");
        kafkaProperties.getStreams().setBootstrapServers(Arrays.asList("localhost:9092"));
        kafkaProperties.getStreams().getProperties().put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        kafkaProperties.getStreams().getProperties().put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        kafkaProperties.getStreams().getProperties().put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test");

        WikipediaTopology topology = new WikipediaTopology(
                wikipediaProperties,
                kafkaProperties
        );

        topology.buildTopology(streamsBuilder);

        FileUtils.deleteDirectory(new File("target/test/streams"));

        Properties config = new Properties();

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        Topology kstreamTopology = streamsBuilder.build();
        System.out.println(kstreamTopology.describe().toString());

        testDriver = new TopologyTestDriver(kstreamTopology, asProperties(kafkaProperties.buildStreamsProperties()));

        inputTopic = testDriver.createInputTopic(
                wikipediaProperties.getTopics().getInput(),
                new StringSerializer(),
                new ByteArraySerializer()
        );

        Deserializer<WikiFeedMetric> deserializer = new SpecificAvroDeserializer<>();
        deserializer.configure(kafkaProperties.buildStreamsProperties(), false);
        outputTopic = testDriver.createOutputTopic(
                wikipediaProperties.getTopics().getOutput(),
                new StringDeserializer(),
                deserializer
        );

    }

    @AfterEach
    public void afterEach() throws Exception {
        testDriver.close();
    }

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

        Assertions.assertThat(outputTopic.getQueueSize())
                .isEqualTo(0);

    }

    private AvroSchema readSchema(String schema) throws Exception {
        return new AvroSchema(FileUtils.readFileToString(
                new File("src/test/avro/"+schema),
                "utf-8"
        ));
    }

    private byte[] readPayload(String message) throws Exception {
        return FileUtils.readFileToByteArray(new File("src/test/data/"+message));
    }

    private Properties asProperties(Map<String, ?> map) {
        Properties properties = new Properties();
        for (Map.Entry<String,?> e : map.entrySet()) {
            properties.put(e.getKey(), e.getValue());
        }
        return properties;
    }

}
