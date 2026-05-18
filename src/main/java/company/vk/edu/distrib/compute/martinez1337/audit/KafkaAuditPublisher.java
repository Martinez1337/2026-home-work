package company.vk.edu.distrib.compute.martinez1337.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaAuditPublisher implements AuditPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAuditPublisher.class);
    private static final String AUDIT_TOPIC = "audit";

    private final AtomicReference<Producer<String, String>> producer = new AtomicReference<>();
    private final AtomicBoolean async = new AtomicBoolean(true);

    @Override
    public void publish(String method, String id, long timestamp) throws IOException {
        Producer<String, String> currentProducer = producer.get();
        if (currentProducer == null) {
            return;
        }

        AuditEvent event = new AuditEvent(method, id, timestamp);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                AUDIT_TOPIC,
                id,
                AuditEventUtils.encode(event)
        );

        if (async.get()) {
            currentProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn("Failed to publish audit event", exception);
                }
            });
            return;
        }

        try {
            currentProducer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while publishing audit event", e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to publish audit event", e);
        }
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            close();
            return;
        }

        Producer<String, String> newProducer = new KafkaProducer<>(producerProperties(bootstrapServers));
        Producer<String, String> oldProducer = producer.getAndSet(newProducer);
        if (oldProducer != null) {
            oldProducer.close();
        }
    }

    @Override
    public void setAsync(boolean enabled) {
        async.set(enabled);
    }

    @Override
    public void close() {
        Producer<String, String> oldProducer = producer.getAndSet(null);
        if (oldProducer != null) {
            oldProducer.close();
        }
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }
}
