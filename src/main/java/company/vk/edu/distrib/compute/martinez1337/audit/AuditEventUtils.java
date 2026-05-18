package company.vk.edu.distrib.compute.martinez1337.audit;

import company.vk.edu.distrib.compute.AuditEvent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class AuditEventUtils {
    private static final String DELIMITER = "\t";

    private AuditEventUtils() {
    }

    public static String encode(AuditEvent event) {
        String id = event.id() == null
                ? ""
                : Base64.getEncoder().encodeToString(event.id().getBytes(StandardCharsets.UTF_8));
        return event.method() + DELIMITER + id + DELIMITER + event.timestamp();
    }

    public static AuditEvent decode(String value) {
        String[] parts = value.split(DELIMITER, -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid audit event payload");
        }

        String id = parts[1].isEmpty()
                ? ""
                : new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        return new AuditEvent(parts[0], id, Long.parseLong(parts[2]));
    }
}
