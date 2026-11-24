import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class RegexConsumer {
    public static void main(String[] args) throws Exception {
        String prefix = args[1];
        String group = prefix + "-group";
        var props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", group);
        props.setProperty("group.protocol", "consumer");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("acks", "all");

        String first = prefix + "-first";
        String second = prefix + "-second";
        String unrelated = prefix + "-other";
        try (var admin = Admin.create(props);
             var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
             var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            create(admin, first);
            create(admin, unrelated);
            producer.send(new ProducerRecord<>(first, "first-value")).get(30, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(unrelated, "not-subscribed")).get(30, TimeUnit.SECONDS);
            consumer.subscribe(new SubscriptionPattern(prefix + "-(?:first|second)"));
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            read(consumer, first, "first-value", offsets);

            create(admin, second);
            producer.send(new ProducerRecord<>(second, "second-value")).get(30, TimeUnit.SECONDS);
            read(consumer, second, "second-value", offsets);
            consumer.commitSync(offsets, Duration.ofSeconds(15));

            var persisted = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
            if (persisted.size() != 2 || persisted.values().stream().anyMatch(offset -> offset.offset() != 1))
                throw new AssertionError("Unexpected committed offsets: " + persisted);
            System.out.println("Server-side pattern and a matching topic created after subscription passed: " + persisted);
        }

        props.setProperty("group.id", group + "-invalid");
        try (var invalid = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            invalid.subscribe(new SubscriptionPattern("(?=" + first + ")"));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            try {
                while (System.nanoTime() < deadline) invalid.poll(Duration.ofMillis(100));
                throw new AssertionError("Broker accepted an invalid RE2/J pattern");
            } catch (InvalidRegularExpression expected) {
                System.out.println("Invalid pattern was rejected by the broker");
            }
        }
    }

    private static void create(Admin admin, String topic) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3))).all().get(30, TimeUnit.SECONDS);
    }

    private static void read(KafkaConsumer<String, String> consumer, String topic, String value,
                             Map<TopicPartition, OffsetAndMetadata> offsets) {
        long deadline = System.nanoTime() + Duration.ofSeconds(35).toNanos();
        while (System.nanoTime() < deadline) {
            boolean found = false;
            for (var record : consumer.poll(Duration.ofMillis(100))) {
                if (!record.topic().equals(topic) || !record.value().equals(value) || record.offset() != 0)
                    throw new AssertionError("Unexpected record: " + record);
                offsets.put(new TopicPartition(topic, record.partition()), new OffsetAndMetadata(record.offset() + 1));
                found = true;
            }
            if (found) return;
        }
        throw new AssertionError("No record for " + topic + ", assignment: " + consumer.assignment());
    }
}
