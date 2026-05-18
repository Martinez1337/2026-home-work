package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.martinez1337.audit.AuditPublisher;
import company.vk.edu.distrib.compute.martinez1337.util.QueryHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static company.vk.edu.distrib.compute.martinez1337.controller.ResponseStatus.*;

public class EntityHttpHandler extends BaseHttpHandler {
    private static final String ID_PARAM_PREFIX = "id";

    private final Dao<byte[]> dao;
    private final AuditPublisher auditPublisher;

    public EntityHttpHandler(Dao<byte[]> dao, AuditPublisher auditPublisher) {
        super();
        this.dao = dao;
        this.auditPublisher = auditPublisher;
    }

    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        log.info("Handle GET request");
        String id = auditAndGetId(exchange);
        byte[] value = dao.get(id);

        exchange.sendResponseHeaders(OK.getCode(), value.length);
        exchange.getResponseBody().write(value);
    }

    @Override
    protected void handlePut(HttpExchange exchange) throws IOException {
        log.info("Handle PUT request");
        String id = auditAndGetId(exchange);
        byte[] value = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, value);

        exchange.sendResponseHeaders(CREATED.getCode(), 0);
    }

    @Override
    protected void handleDelete(HttpExchange exchange) throws IOException {
        log.info("Handle DELETE request");
        String id = auditAndGetId(exchange);
        dao.delete(id);

        exchange.sendResponseHeaders(ACCEPTED.getCode(), 0);
    }

    private String auditAndGetId(HttpExchange exchange) throws IOException {
        long timestamp = System.currentTimeMillis();
        Map<String, List<String>> params = QueryHelper.parseParams(exchange.getRequestURI().getRawQuery());

        if (params.isEmpty() || !params.containsKey(ID_PARAM_PREFIX)) {
            throw new IllegalArgumentException("bad query");
        }

        String id = params.get(ID_PARAM_PREFIX).getFirst();
        auditPublisher.publish(exchange.getRequestMethod(), id, timestamp);
        return id;
    }
}
