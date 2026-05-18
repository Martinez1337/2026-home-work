package company.vk.edu.distrib.compute.martinez1337.audit;

import java.io.Closeable;
import java.io.IOException;

public interface AuditPublisher extends Closeable {
    void publish(String method, String id, long timestamp) throws IOException;

    void setBootstrapServers(String bootstrapServers);

    void setAsync(boolean enabled);

    @Override
    void close();
}
