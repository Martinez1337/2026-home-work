package company.vk.edu.distrib.compute.martinez1337.service;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.martinez1337.audit.AuditPublisher;
import company.vk.edu.distrib.compute.martinez1337.audit.KafkaAuditPublisher;
import company.vk.edu.distrib.compute.martinez1337.controller.EntityHttpHandler;
import company.vk.edu.distrib.compute.martinez1337.controller.StatusHandler;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Martinez1337KVService implements AuditableKVService {
    private final Dao<byte[]> dao;
    private final HttpServer server;
    private final AuditPublisher auditPublisher;

    public Martinez1337KVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.auditPublisher = new KafkaAuditPublisher();
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHttpHandler(dao, auditPublisher));
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(1);
        auditPublisher.close();
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        auditPublisher.setBootstrapServers(bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        auditPublisher.setAsync(enabled);
    }
}
