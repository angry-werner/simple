package ch.kow.kafka.simple.producer;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;

public class ProducerConsumer {
    private final KafkaProducer<String, String> kafkaProducer;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final AdminClient adminClient;
    private final ThreadPoolExecutor threadPoolExecutor;

    private ProducerConsumer() throws UnknownHostException {
        super();
        kafkaProducer = createProducer();
        kafkaConsumer = createConsumer();
        adminClient = createAdminClient();
        threadPoolExecutor = createThreadpoolExecutor();
    }

    public static void main(final String[] args) throws Exception {
        final ProducerConsumer producerConsumer = new ProducerConsumer();
        final Set<String> names = producerConsumer.adminClient.listTopics().names().get();
        System.out.println("names:" + names.toString());
        producerConsumer.threadPoolExecutor.execute(producerConsumer.createEventProducer());
        producerConsumer.threadPoolExecutor.execute(producerConsumer.createEventConsumer());
    }

    private Future<RecordMetadata> send(final String key, final String value) {
        try {
            return kafkaProducer.send(new ProducerRecord<>("foo", key, value));
        } finally {
            kafkaProducer.flush();
        }
    }

    private KafkaProducer<String, String> createProducer() throws UnknownHostException {
        final Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("bootstrap.servers", "louie:29092");
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(config);
    }

    private KafkaConsumer<String, String> createConsumer() {
        final Properties config = new Properties();
        config.put("bootstrap.servers", "louie:29092");
        config.put("group.id", "FooConsumer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList("foo"));
        return consumer;
    }

    private AdminClient createAdminClient() throws UnknownHostException {
        final Properties adminProperties = new Properties();
        adminProperties.put("client.id", InetAddress.getLocalHost().getHostName());
        adminProperties.put("bootstrap.servers", "louie:29092");
        return AdminClient.create(adminProperties);
    }

    private ThreadPoolExecutor createThreadpoolExecutor() {
        return new ThreadPoolExecutor(10, 10, 1000l, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(10));
    }

    private Runnable createEventProducer() {
        return () -> {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    System.out.println("Key: ");
                    final String key = reader.readLine();
                    if (key == null || key.length() == 0) {
                        break;
                    }
                    System.out.println("Value: ");
                    final String value = reader.readLine();
                    if (value == null || value.length() == 0) {
                        break;
                    }
                    final Future<RecordMetadata> result = send(key, value);
                    System.out.println("Offset: " + result.get().offset());
                }
            } catch (final IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable createEventConsumer() {
        return () -> {
            while (true) {
                final ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (final ConsumerRecord<String, String> record: records) {
                    System.out.println("Received key: " + record.key() + ", value: " + record.value());
                }
            }
        };
    }
}
