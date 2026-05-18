package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.martinez1337.audit.AuditEventUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Martinez1337AuditService implements AuditService {
    private static final Logger LOG = LoggerFactory.getLogger(Martinez1337AuditService.class);
    private static final String AUDIT_TOPIC = "audit";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<KafkaConsumer<String, String>> consumer = new AtomicReference<>();
    private final AtomicReference<Thread> worker = new AtomicReference<>();

    public Martinez1337AuditService(String bootstrapServers, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        KafkaConsumer<String, String> newConsumer = new KafkaConsumer<>(consumerProperties());
        newConsumer.subscribe(List.of(AUDIT_TOPIC));
        consumer.set(newConsumer);

        Thread newWorker = new Thread(() -> consume(newConsumer), "martinez1337-audit-consumer");
        newWorker.setDaemon(true);
        worker.set(newWorker);
        newWorker.start();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        KafkaConsumer<String, String> currentConsumer = consumer.get();
        if (currentConsumer != null) {
            currentConsumer.wakeup();
        }

        Thread currentWorker = worker.get();
        if (currentWorker != null) {
            try {
                currentWorker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return new ArrayList<>(events);
    }

    private void consume(KafkaConsumer<String, String> currentConsumer) {
        try (currentConsumer) {
            while (running.get()) {
                ConsumerRecords<String, String> records = currentConsumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    continue;
                }

                records.forEach(record -> events.add(AuditEventUtils.decode(record.value())));
                currentConsumer.commitSync();
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } catch (RuntimeException e) {
            LOG.warn("Audit consumer stopped unexpectedly.", e);
        } finally {
            try {
                currentConsumer.commitSync();
            } catch (RuntimeException e) {
                LOG.debug("Failed to commit audit offsets while stopping.", e);
            }
            consumer.set(null);
            worker.set(null);
            running.set(false);
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
