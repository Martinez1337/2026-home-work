package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;

public class Martinez1337AuditServiceFactory extends AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new Martinez1337AuditService(bootstrapServers, consumerGroupId);
    }
}
