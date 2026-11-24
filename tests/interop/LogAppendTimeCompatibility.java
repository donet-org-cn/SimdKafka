import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.record.TimestampType;

public class LogAppendTimeCompatibility {
    public static void main(String[] args) throws Exception {
        String mode = args[1];
        Path evidence = Path.of(args[2]);
        short replicas = Short.parseShort(args[3]);
        boolean transactional = replicas > 1;
        var config = new Properties();
        config.put("bootstrap.servers", args[0]);
        config.put("request.timeout.ms", "10000");
        config.put("default.api.timeout.ms", "20000");
        var topic = new TopicPartition("native-append-time", 0);
        try (var admin = Admin.create(config)) {
            if (mode.equals("seed")) {
                admin.createTopics(List.of(new NewTopic(topic.topic(), 1, replicas)
                    .configs(Map.of("message.timestamp.type", "LogAppendTime")))).all().get(25, TimeUnit.SECONDS);
                config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                config.put("compression.type", "gzip");
                config.put("linger.ms", "50");
                if (transactional) config.put("transactional.id", "native-timestamp");
                long timestamp;
                try (var producer = new KafkaProducer<String, String>(config)) {
                    if (transactional) {
                        producer.initTransactions();
                        producer.beginTransaction();
                    }
                    long before = System.currentTimeMillis();
                    var a = producer.send(new ProducerRecord<>(topic.topic(), 0, 10L, "a", "first"));
                    var b = producer.send(new ProducerRecord<>(topic.topic(), 0, 5L, "b", "second"));
                    var first = a.get(20, TimeUnit.SECONDS);
                    var second = b.get(20, TimeUnit.SECONDS);
                    if (transactional) producer.commitTransaction();
                    require(first.offset() == 0 && second.offset() == 1 &&
                        first.timestamp() >= before && first.timestamp() <= System.currentTimeMillis() &&
                        second.timestamp() == first.timestamp(), "broker append timestamp is missing from Produce acks");
                    timestamp = first.timestamp();
                }
                Files.writeString(evidence, Long.toString(timestamp));
                System.out.println("Native Kafka 4.4 LogAppendTime GZIP append acks passed, replicas=" + replicas);
            } else {
                long timestamp = Long.parseLong(Files.readString(evidence).strip());
                var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.topic());
                require(admin.describeConfigs(List.of(resource)).all().get(20, TimeUnit.SECONDS).get(resource)
                    .get("message.timestamp.type").value().equals("LogAppendTime"), "timestamp policy changed after restart");
                var position = admin.listOffsets(Map.of(topic, OffsetSpec.forTimestamp(timestamp))).partitionResult(topic)
                    .get(20, TimeUnit.SECONDS);
                require(position.offset() == 0 && position.timestamp() == timestamp, "ListOffsets returned a different timestamp or offset");
                config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                if (transactional) config.put("isolation.level", "read_committed");
                try (var consumer = new KafkaConsumer<String, String>(config)) {
                    consumer.assign(List.of(topic));
                    consumer.seek(topic, 0);
                    var records = new ArrayList<ConsumerRecord<String, String>>();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (records.size() < 2 && System.nanoTime() < deadline)
                        consumer.poll(Duration.ofMillis(300)).forEach(records::add);
                    require(records.size() == 2 && records.get(0).value().equals("first") && records.get(1).value().equals("second") &&
                        records.stream().allMatch(record -> record.timestamp() == timestamp &&
                            record.timestampType() == TimestampType.LOG_APPEND_TIME), "consumed payload or timestamp changed after restart");
                }
                System.out.println("Native Kafka 4.4 timestamp lookup and fetch after restart passed, replicas=" + replicas);
            }
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
