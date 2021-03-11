package dev.psmolinski.kafka.wikipedia;

import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.commons.io.FileUtils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This test shows the same scenario when one message results in application output and another
 * one is discarded. The goal of such test case is to verify that the application is started
 * properly and the major communication paths are working. Therefore ideally we should check
 * only some paths that we know that they produce specific output. Testing for absence
 * could be done in different types of tests. This test can be also used against shared environment.
 * In such case we should make use of correlation ids and consider the output state to be
 * depending on other input messages.
 */
public class WikipediaProcessorIT {

    // These topics MUST be same as the topics defined in application.yml.
    private static final String INPUT_TOPIC = "wikipedia.parsed";
    private static final String OUTPUT_TOPIC = "wikipedia.parsed.count-by-domain";

    // We use regular Kafka API
    private KafkaProducer<String, GenericRecord> producer;
    private KafkaConsumer<String, GenericRecord> consumer;

    @BeforeEach
    public void beforeEach() {

        Serializer<GenericRecord> serializer = new GenericAvroSerializer();
        serializer.configure(
                MapBuilder.newBuilder(String.class, String.class)
                        .with("schema.registry.url", "http://localhost:8081")
                .build(),
                false
        );
        producer = new KafkaProducer<>(
                MapBuilder.newBuilder(String.class, Object.class)
                        .with("bootstrap.servers", "localhost:9093")
                        .with("acks", "all")
                        // non-transactional, but at least idempotent
                        .with("enable.idempotence", "true")
                        .build(),
                new StringSerializer(),
                serializer
        );

        Deserializer<GenericRecord> deserializer = new GenericAvroDeserializer();
        deserializer.configure(
                MapBuilder.newBuilder(String.class, String.class)
                        .with("schema.registry.url", "http://localhost:8081")
                        .build(),
                false
        );
        consumer = new KafkaConsumer<>(
                MapBuilder.newBuilder(String.class, Object.class)
                        .with("bootstrap.servers", "localhost:9093")
                        // KStreams uses EOS, therefore there could be potentially rolled back messages
                        .with("isolation.level", "read_committed")
                        // note missing group.id; this is due to usage of 'assign' instead of 'subscribe'
                        .build(),
                new StringDeserializer(),
                deserializer
        );

    }

    @AfterEach
    public void afterEach() {

        if (consumer!=null) {
            consumer.close();
            consumer = null;
        }

        if (producer!=null) {
            producer.close();
            producer = null;
        }

    }

    // ---- tests ----

    /**
     * Example test verifying that a message sent to the application is processed and a result
     * is produced. For comparison it also includes test for missing records.
     * <br/>
     * This test explicitly consumes messages from the output topic. There is no need to
     * keep the consumer poll loop because the consumption is local, i.e. the consumer
     * is not coordinated within group.
     */
    @Test
    public void testSample() throws Exception {

        // read topic partitions
        List<TopicPartition> partitions = consumer.partitionsFor(OUTPUT_TOPIC)
                .stream()
                .map(p->new TopicPartition(p.topic(),p.partition()))
                .collect(Collectors.toList());

        // we are not using coordinated consumer groups, so no 'subscribe' calls
        consumer.assign(partitions);
        // here we use manual offset management and we are reading only new messages
        consumer.seekToEnd(partitions);

        // to ignore the preamble we need the explicit schema
        Schema schema = readSchema("schema.avsc");

        // send message, fail when sending fails
        send(producer, new ProducerRecord<>(INPUT_TOPIC, readMessage("0-00000.avro", schema))).get();

        // try reading at least one message; it is a good idea to use correlation ids in the header
        // and filter out irrelevant messages
        // in this case we expect to get the output message shortly after sending the input
        List<ConsumerRecord<String,GenericRecord>> records1 = receiveAny(consumer, 10_000);

        Assertions.assertThat(records1)
                .hasSize(1);
        Assertions.assertThat(records1.get(0).value().get("domain"))
                .isEqualTo("www.wikidata.org");
        // here is a side effect value and the test should not assume it is always 1
        // the assertion is kept for comparison of the same scenario in other techniques
        Assertions.assertThat(records1.get(0).value().get("editCount"))
                .isEqualTo(1L);

        // this message is filtered out by the application
        // in practice tests for missing messages should not be executed in this kind of tests
        // because they introduce unavoidable waits for timeouts
        send(producer, new ProducerRecord<>(INPUT_TOPIC, readMessage("0-00001.avro", schema))).get();

        List<ConsumerRecord<String,GenericRecord>> records2 = receiveAny(consumer, 10_000);

        Assertions.assertThat(records2)
                .hasSize(0);

    }


    // ---- utilities ----

    /**
     * Send all the provided producer records at once, force sending with {@link KafkaProducer#flush()}
     * and return the future reflecting the cumulative result. If any records fail, the result
     * future also fails with arbitrary exception from the failed records.
     * @param producer
     * @param records
     * @param <K>
     * @param <V>
     * @return
     */
    private <K,V> CompletableFuture<Void> send(Producer<K,V> producer, ProducerRecord<K, V>...records) {
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>(records.length);
        for (ProducerRecord<K,V> record : records) {
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            producer.send(record, (m,e)-> {
                if (e!=null) {
                    future.completeExceptionally(e);
                } else {
                    future.complete(m);
                }
            });
            futures.add(future);
        }
        producer.flush();
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Poll the consumer until at least one record is received or the timeout passes.
     * The consumer is not coordinated member of the group, therefore there is no need
     * to poll in a tight loop.
     * <br/>
     * Some good ideas:
     * <ul>
     *     <li>filter out messages not matching predicates</li>
     *     <li>implement {@code receiveAll} that does the same, but consumes all until deadline</li>
     * </ul>
     * @param consumer
     * @param timeout
     * @param <K>
     * @param <V>
     * @return
     */
    private <K,V> List<ConsumerRecord<K,V>> receiveAny(Consumer<K,V> consumer, long timeout) {
        List<ConsumerRecord<K, V>> results = new ArrayList<>();
        long deadline = System.currentTimeMillis()+timeout;
        for (;;) {
            long remaining = deadline-System.currentTimeMillis();
            if (remaining<=0) break;
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(remaining));
            for (ConsumerRecord<K, V> record : records) {
                results.add(record);
            }
            if (!results.isEmpty()) break;
        }
        return results;
    }

    /**
     * Read Avro schema from the file.
     */
    private static Schema readSchema(String schema) throws Exception {
        return new Schema.Parser().parse(new File("src/test/avro/"+schema));
    }

    /**
     * Read and parse Avro message. The stored message payloads are captured from actual topic,
     * therefore they contain 5-octets preamble. This method uses predefined schema and the preamble
     * can be ignored.
     * @param message file name in src/test/data directory
     * @param schema actual Avro schema corresponding to the one referenced by the id from preamble
     */
    private static GenericRecord readMessage(String message, Schema schema) throws Exception {
        byte[] buffer = FileUtils.readFileToByteArray(new File("src/test/data/"+message));
        DatumReader<GenericRecord> reader = GenericData.get().createDatumReader(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(
                buffer, 5, buffer.length-5,
                null);
        return reader.read(null, decoder);
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
