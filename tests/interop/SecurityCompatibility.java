import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class SecurityCompatibility {
    public static void main(String[] args) throws Exception {
        String address = args[0];
        Path files = Path.of(args[1]);
        String mode = args[2];
        var oauth = config(address, files);
        oauth.put("sasl.mechanism", "OAUTHBEARER");
        oauth.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        oauth.put("sasl.login.callback.handler.class", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
        String endpoint = files.resolve("token.jwt").toUri().toString();
        System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", endpoint);
        oauth.put("sasl.oauthbearer.token.endpoint.url", endpoint);
        var token = config(address, files);
        token.put("sasl.mechanism", "SCRAM-SHA-256");
        var topic = new TopicPartition("native-security", 0);
        Path state = files.resolve("delegation.properties");

        try (var admin = Admin.create(oauth)) {
            if (mode.equals("issue")) {
                admin.createTopics(List.of(new NewTopic(topic.topic(), 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
                var created = admin.createDelegationToken(new CreateDelegationTokenOptions()
                    .owner(new KafkaPrincipal("User", "admin"))).delegationToken().get(20, TimeUnit.SECONDS);
                require(created.tokenInfo().owner().getName().equals("admin"), "OAuth subject was not mapped to token owner");
                require(admin.describeDelegationToken().delegationTokens().get(20, TimeUnit.SECONDS).stream()
                    .anyMatch(value -> value.tokenInfo().tokenId().equals(created.tokenInfo().tokenId())), "delegation did not describe");
                var saved = new Properties();
                saved.setProperty("id", created.tokenInfo().tokenId());
                saved.setProperty("hmac", created.hmacAsBase64String());
                try (OutputStream output = Files.newOutputStream(state)) { saved.store(output, null); }
                delegate(token, saved);
                admin.renewDelegationToken(created.hmac()).expiryTimestamp().get(20, TimeUnit.SECONDS);
                token.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                token.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                try (var producer = new KafkaProducer<String, String>(token)) {
                    require(producer.send(new ProducerRecord<>(topic.topic(), 0, "delegated", "native-4.4"))
                        .get(20, TimeUnit.SECONDS).offset() == 0, "native producer write failed");
                }
                System.out.println("Native Kafka 4.4 OAuth, Create/Describe/RenewDelegationToken and SCRAM produce passed");
            } else {
                var saved = new Properties();
                try (InputStream input = Files.newInputStream(state)) { saved.load(input); }
                delegate(token, saved);
                token.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                token.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                try (var consumer = new KafkaConsumer<String, String>(token)) {
                    consumer.assign(List.of(topic));
                    consumer.seek(topic, 0);
                    var records = consumer.poll(Duration.ofSeconds(15)).records(topic);
                    require(records.size() == 1 && records.getFirst().value().equals("native-4.4"), "delegated read after restart failed");
                }
                admin.expireDelegationToken(java.util.Base64.getDecoder().decode(saved.getProperty("hmac")))
                    .expiryTimestamp().get(20, TimeUnit.SECONDS);
                try (var rejected = Admin.create(token)) {
                    try {
                        rejected.listTopics().names().get(20, TimeUnit.SECONDS);
                        throw new IllegalStateException("revoked delegation could authenticate");
                    } catch (ExecutionException expected) {
                        if (!(expected.getCause() instanceof SaslAuthenticationException)) throw expected;
                    }
                }
                System.out.println("Native Kafka 4.4 SCRAM fetch after restart, ExpireDelegationToken and revoked login passed");
            }
        }
    }

    private static Properties config(String address, Path files) {
        var properties = new Properties();
        properties.putAll(Map.of("bootstrap.servers", address, "security.protocol", "SASL_SSL",
            "ssl.truststore.type", "PEM", "ssl.truststore.location", files.resolve("ca.pem").toString(),
            "request.timeout.ms", "8000", "default.api.timeout.ms", "15000"));
        return properties;
    }

    private static void delegate(Properties config, Properties saved) {
        config.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" +
            saved.getProperty("id") + "\" password=\"" + saved.getProperty("hmac") + "\" tokenauth=\"true\";");
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
