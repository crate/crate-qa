package io.crate.qa.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class KafkaJdbcConnectorCrateDBIntegrationTest {

    static final String PSQL_DRIVER = "jdbc:postgresql://localhost:54432/doc?user=crate";

    @Test
    public void test_kafka_jdbc_connector_cratedb_roundtrip() throws Exception {

        createMetricsTablesInCrateDB();
        waitUntil(this::setupKafkConnector);
        waitUntil(this::isKafkaConnectorReady);

        var schema = ReflectData.get().getSchema(Metrics.class);

        var producer = getProducer();

        var m1 = new Metrics(1, 1);
        var m2 = new Metrics(2, 2);
        var m3 = new Metrics(3, 3);
        var m4 = new Metrics(4, 4);
        var m5 = new Metrics(5, 5);

        producer.send(toProducerRecord(schema, m1));
        producer.send(toProducerRecord(schema, m2));
        producer.send(toProducerRecord(schema, m3));
        producer.send(toProducerRecord(schema, m4));
        producer.send(toProducerRecord(schema, m5));

        producer.flush();
        producer.close();

        assertBusy(() -> {
            var results = fetchResultsFromCrateDB();
            assertThat(results.size(), is(5));
            assertThat(results.get(0), Is.is(m1));
            assertThat(results.get(1), Is.is(m2));
            assertThat(results.get(2), Is.is(m3));
            assertThat(results.get(3), Is.is(m4));
            assertThat(results.get(4), Is.is(m5));
        }, 20, TimeUnit.SECONDS);
    }

    ProducerRecord<Object, Object> toProducerRecord(Schema schema, Metrics metric) throws IOException {
        return new ProducerRecord<>("metrics", toGenericRecord(schema, metric));
    }

    GenericRecord toGenericRecord(Schema schema, Metrics metric) throws IOException {
        try (var outputStream = new ByteArrayOutputStream()) {
            var datumWriter = new ReflectDatumWriter<>(schema);
            var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(metric, encoder);
            encoder.flush();
            var datumReader = new GenericDatumReader<GenericRecord>(schema);
            var decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
            return datumReader.read(null, decoder);
        }
    }

     List<Metrics> fetchResultsFromCrateDB() throws Exception {
        try(var conn = DriverManager.getConnection(PSQL_DRIVER)) {
            var stmt = conn.createStatement();
            stmt.execute("refresh table metrics;");
            try(var rs = stmt.executeQuery("select * from metrics order by id")) {
                var results = new ArrayList<Metrics>();
                while (rs.next()) {
                    results.add(new Metrics(rs.getInt("id"), rs.getInt("x")));
                }
                return results;
            }
        }
    }

    void createMetricsTablesInCrateDB() throws Exception {
        try(var conn = DriverManager.getConnection(PSQL_DRIVER)) {
            var stmt = conn.createStatement();
            stmt.execute("drop table if exists metrics;");
            stmt.execute("create table metrics (id int primary key, x int);");
        }
    }

    boolean setupKafkConnector() {
        var configuration = Map.of(
                "connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector",
                "topics", "metrics",
                "connection.url", "jdbc:postgresql://cratedb:5432/doc?user=crate",
                "tasks.max", 1,
                "pk.mode", "record_value",
                "pk.fields", "id",
                "insert.mode", "upsert"
        );

        try {
            var request = httpRequest()
                    .PUT(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(configuration)))
                    .build();
            return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (Exception ignored) {
            return false;
        }
    }

    void waitUntil(Supplier<Boolean> f) throws Exception {
        for (double i = 0; i < 5; i++) {
            if (!f.get()) {
                Thread.sleep((int) Math.pow(10d, i));
            } else {
                return;
            }
        }
        throw new RuntimeException("Retries to connect exceeded");
    }

    boolean isKafkaConnectorReady() {
        try {
            var response = HttpClient.newHttpClient().send(httpRequest().GET().build(), HttpResponse.BodyHandlers.ofString());
            Map<String, Object> result = new ObjectMapper().readValue(response.body(), new TypeReference<>() {
            });
            var topics = result.get("topics");
            return topics != null && topics.equals("metrics");
        } catch (Exception ignored) {
            return false;
        }
    }

    HttpRequest.Builder httpRequest() {
        return HttpRequest
                .newBuilder()
                .uri(URI.create("http://localhost:8083/connectors/jdbc-sink-connector/config"))
                .header("Content-Type", "application/json");
    }

    Producer<Object, Object> getProducer() {
        var producerProps = new Properties();
        producerProps.put("bootstrap.servers", "127.0.0.1:9092");
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", KafkaAvroSerializer.class.getName());
        producerProps.put("value.serializer", KafkaAvroSerializer.class.getName());
        producerProps.put("linger.ms", "10");
        producerProps.put("schema.registry.url", "http://127.0.0.1:8081");
        return new KafkaProducer<>(producerProps);
    }

    static void assertBusy(CheckedRunnable<Exception> codeBlock, long maxWaitTime, TimeUnit unit) throws Exception {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long iterations = Math.max(Math.round(Math.log10(maxTimeInMillis) / Math.log10(2)), 1);
        long timeInMillis = 1;
        long sum = 0;
        List<AssertionError> failures = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            try {
                codeBlock.run();
                return;
            } catch (AssertionError e) {
                failures.add(e);
            }
            sum += timeInMillis;
            Thread.sleep(timeInMillis);
            timeInMillis *= 2;
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        try {
            codeBlock.run();
        } catch (AssertionError e) {
            for (AssertionError failure : failures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    @FunctionalInterface
    public interface CheckedRunnable<E extends Exception> {
        void run() throws E;
    }

}