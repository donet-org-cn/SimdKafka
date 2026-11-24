import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ShareGroupCompatibility {
    private static final String TOPIC = "share-interop-events";
    private static final String GROUP = "share-interop-group";

    public static void main(String[] args) throws Exception {
        Properties base = new Properties();
        base.setProperty("bootstrap.servers", args[0]);
        base.setProperty("request.timeout.ms", "8000");
        base.setProperty("default.api.timeout.ms", "15000");
        Properties producerSettings = new Properties();
        producerSettings.putAll(base);
        producerSettings.put("key.serializer", StringSerializer.class);
        producerSettings.put("value.serializer", StringSerializer.class);
        try (Admin admin = Admin.create(base); KafkaProducer<String, String> producer = new KafkaProducer<>(producerSettings)) {
            if (args.length > 1 && args[1].equals("resume")) {
                try (var resumed = consumer(base, "resume")) {
                    resumed.subscribe(List.of(TOPIC));
                    check(resumed.poll(Duration.ofSeconds(4)).isEmpty(), "completed records were delivered again after restart");
                    send(producer, "after-restart");
                    ConsumerRecord<String, String> record = next(resumed, "after-restart", 1);
                    check(record.offset() == 3, "offset changed after restart");
                    acknowledge(resumed, record, AcknowledgeType.ACCEPT);
                }
                checkOffset(admin, 4);
                admin.deleteShareGroupOffsets(GROUP, Set.of(TOPIC)).all().get(15, TimeUnit.SECONDS);
                var remaining = admin.listShareGroupOffsets(Map.of(GROUP, new ListShareGroupOffsetsSpec()))
                    .all().get(15, TimeUnit.SECONDS).get(GROUP);
                check(remaining.isEmpty(), "the deleted share-topic cursor is still present");
                System.out.println("KafkaShareConsumer recovered the durable cursor and accepted offset 3");
                return;
            }
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
            send(producer, "release-and-retry");
            send(producer, "parallel-accept");
            admin.alterShareGroupOffsets(GROUP, Map.of(new TopicPartition(TOPIC, 0), 0L)).all().get(15, TimeUnit.SECONDS);
            try (var a = consumer(base, "first"); var b = consumer(base, "second")) {
                a.subscribe(List.of(TOPIC));
                b.subscribe(List.of(TOPIC));
                var released = next(a, "release-and-retry", 1);
                var parallel = next(b, "parallel-accept", 1);
                check(released.offset() == 0 && parallel.offset() == 1, "consumers did not share the same partition");
                acknowledge(b, parallel, AcknowledgeType.ACCEPT);
                acknowledge(a, released, AcknowledgeType.RELEASE);
                var retried = next(b, "release-and-retry", 2);
                check(retried.offset() == released.offset(), "released record moved to another offset");
                acknowledge(b, retried, AcknowledgeType.ACCEPT);
                send(producer, "reject-permanently");
                acknowledge(a, next(a, "reject-permanently", 1), AcknowledgeType.REJECT);
            }
            check(admin.describeShareGroups(List.of(GROUP)).all().get(15, TimeUnit.SECONDS).containsKey(GROUP),
                "native Admin could not describe the share group");
            checkOffset(admin, 3);
            System.out.println("Two native consumers shared one partition and confirmed ACCEPT, RELEASE, redelivery and REJECT");
        }
    }

    private static KafkaShareConsumer<String, String> consumer(Properties base, String name) {
        Properties properties = new Properties();
        properties.putAll(base);
        properties.setProperty("group.id", GROUP);
        properties.setProperty("client.id", name);
        properties.setProperty("max.poll.records", "1");
        properties.setProperty("share.acknowledgement.mode", "explicit");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        return new KafkaShareConsumer<>(properties);
    }

    private static void send(KafkaProducer<String, String> producer, String value) throws Exception {
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value)).get(15, TimeUnit.SECONDS);
    }

    private static void checkOffset(Admin admin, long expected) throws Exception {
        var result = admin.listShareGroupOffsets(Map.of(GROUP, new ListShareGroupOffsetsSpec()))
            .all().get(15, TimeUnit.SECONDS).get(GROUP).get(new TopicPartition(TOPIC, 0));
        check(result != null && result.startOffset() == expected && result.lag().orElseThrow() == 0,
            "share admin returned the wrong start offset or lag");
    }

    private static ConsumerRecord<String, String> next(KafkaShareConsumer<String, String> consumer, String value, int attempts) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(18);
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) continue;
            check(records.count() == 1, "the record limit was exceeded");
            var record = records.iterator().next();
            check(record.partition() == 0 && record.value().equals(value), "unexpected record " + record.value());
            check(record.deliveryCount().orElseThrow() == attempts, "unexpected delivery count for " + value);
            return record;
        }
        throw new IllegalStateException("Timed out waiting for " + value);
    }

    private static void acknowledge(KafkaShareConsumer<String, String> consumer, ConsumerRecord<String, String> record, AcknowledgeType type) {
        consumer.acknowledge(record, type);
        var result = consumer.commitSync(Duration.ofSeconds(10));
        check(!result.isEmpty(), "commit returned no partition for " + type);
        for (var error : result.values()) if (error.isPresent()) throw error.get();
    }

    private static void check(boolean valid, String message) {
        if (!valid) throw new IllegalStateException(message);
    }
}
