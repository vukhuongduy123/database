package com.duyvu.database.server;

import com.duyvu.database.engine.DatabaseEngine;
import com.duyvu.database.queryparser.QueryParser;
import com.duyvu.database.result.QueryResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.log4j.Log4j2;
import tools.jackson.databind.json.JsonMapper;

@Log4j2
public final class Server {
  private final int port;
  private final ExecutorService executorService;
  private final DatabaseEngine databaseEngine = DatabaseEngine.getInstance();
  private final JsonMapper mapper = new JsonMapper();

  static final int HTTP_OK = 200;
  static final int HTTP_INTERNAL_SERVER_ERROR = 500;
  static final int HTTP_BAD_REQUEST = 400;

  public Server(int port, int numberOfThreads) {
    this.port = port;
    this.executorService = Executors.newFixedThreadPool(numberOfThreads);
  }

  public void start() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.setExecutor(executorService);
    server.createContext("/query", this::handle);
    log.info("Server started on port {}", port);
    server.start();
  }

  private void handle(HttpExchange exchange) {
    try {
      RequestQuery request;

      try (InputStream is = exchange.getRequestBody()) {
        request = mapper.readValue(is, RequestQuery.class);
      }
      log.debug("Received request: {}", request);

      if (request == null || request.getQuery() == null) {
        sendJson(exchange, HTTP_BAD_REQUEST, "{\"error\":\"missing query\"}");
        return;
      }

      QueryResult result =
          databaseEngine.execute(QueryParser.parseCommand(request.getQuery()));

      String body = mapper.writeValueAsString(result);
      sendJson(exchange, HTTP_OK, body);

    } catch (Exception e) {
      log.error("Request failed", e);
      try {
        sendJson(exchange, HTTP_INTERNAL_SERVER_ERROR, "{\"error\":\"internal server error\"}");
      } catch (IOException ignored) {}
    }
  }

  private void sendJson(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);

    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
