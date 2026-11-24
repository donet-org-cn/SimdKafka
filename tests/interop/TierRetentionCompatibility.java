import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

public class TierRetentionCompatibility {
    public static void main(String[] args) throws Exception {
        var config = new Properties();
        config.put("bootstrap.servers", args[0]);
        config.put("request.timeout.ms", "8000");
        config.put("default.api.timeout.ms", "15000");
        var topic = new TopicPartition("native-age-tier", 0);
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.topic());
        try (var admin = Admin.create(config)) {
            if (args[1].equals("seed")) {
                admin.createTopics(List.of(new NewTopic(topic.topic(), 1, (short) 1).configs(Map.of(
                    "remote.storage.enable", "true", "segment.bytes", "4096", "local.retention.bytes", "-1"))))
                    .all().get(15, TimeUnit.SECONDS);
                config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                long now = Instant.now().toEpochMilli();
                try (var producer = new KafkaProducer<String, String>(config)) {
                    producer.send(new ProducerRecord<>(topic.topic(), 0, now - 120_000, "old", "archived"))
                        .get(15, TimeUnit.SECONDS);
                    producer.send(new ProducerRecord<>(topic.topic(), 0, now + 600_000, "new", "local"))
                        .get(15, TimeUnit.SECONDS);
                }
                require(offset(admin, topic, OffsetSpec.earliestLocal()) == 0, "both records initially local");
                admin.incrementalAlterConfigs(Map.of(resource, List.of(
                    new AlterConfigOp(new ConfigEntry("local.retention.ms", "30000"), AlterConfigOp.OpType.SET))))
                    .all().get(15, TimeUnit.SECONDS);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (offset(admin, topic, OffsetSpec.earliestLocal()) != 1) {
                    require(System.nanoTime() < deadline, "aged records did not leave the local tier");
                    Thread.sleep(100);
                }
                System.out.println("Native Kafka 4.4 timed tier archived one old batch without further writes");
            } else {
                require(offset(admin, topic, OffsetSpec.earliest()) == 0, "global start offset changed");
                require(offset(admin, topic, OffsetSpec.earliestLocal()) == 1, "local start offset was not persisted");
                require(offset(admin, topic, OffsetSpec.latestTiered()) == 0, "tiered boundary was not persisted");
                require(admin.describeConfigs(List.of(resource)).all().get(15, TimeUnit.SECONDS)
                    .get(resource).get("local.retention.ms").value().equals("30000"), "updated retention was not persisted");
                config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                try (var consumer = new KafkaConsumer<String, String>(config)) {
                    consumer.assign(List.of(topic));
                    consumer.seek(topic, 0);
                    var values = new ArrayList<String>();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (values.size() < 2 && System.nanoTime() < deadline)
                        consumer.poll(Duration.ofMillis(300)).forEach(record -> values.add(record.value()));
                    require(values.equals(List.of("archived", "local")), "cross-tier read changed record order or content");
                }
                System.out.println("Native Kafka 4.4 timed tier preserved config, offsets and both records after restart");
            }
        }
    }

    private static long offset(Admin admin, TopicPartition topic, OffsetSpec spec) throws Exception {
        return admin.listOffsets(Map.of(topic, spec)).partitionResult(topic).get(10, TimeUnit.SECONDS).offset();
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
