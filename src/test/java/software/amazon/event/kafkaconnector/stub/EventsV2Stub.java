/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.stub;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Minimal test stub of the EventBridge Custom Event Bus (eventsv2) {@code PutRawEvents} operation.
 * Speaks Smithy RPC v2 CBOR, records every request, and accepts every entry as {@code PUBLISHED}.
 * Recorded requests are exposed as JSON on {@code GET /_stub/requests} for integration tests.
 *
 * <p>Test-only and dependency-free so it can run from compiled test classes in a plain JRE
 * container. It performs no authentication: it binds to loopback unless {@code STUB_BIND} says
 * otherwise, which the e2e compose files set only inside the compose network while publishing the
 * port to {@code 127.0.0.1}.
 */
public final class EventsV2Stub {

  static final String PUT_RAW_EVENTS_PATH = "/service/AWSEventsV2/operation/PutRawEvents";

  /** A recorded PutRawEvents request. */
  public record RecordedRequest(String eventBusArn, List<RecordedEntry> entries) {}

  /** A recorded request entry. */
  public record RecordedEntry(
      byte[] data, Map<String, String> metadata, Map<String, String> systemMetadata) {}

  private final List<RecordedRequest> requests = Collections.synchronizedList(new ArrayList<>());
  private final HttpServer server;

  private EventsV2Stub(HttpServer server) {
    this.server = server;
  }

  public static EventsV2Stub start(InetSocketAddress address) throws IOException {
    var stub = new EventsV2Stub(HttpServer.create(address, 0));
    stub.server.createContext(PUT_RAW_EVENTS_PATH, stub::handlePutRawEvents);
    stub.server.createContext("/_stub/requests", stub::handleListRequests);
    stub.server.createContext("/_stub/health", stub::handleHealth);
    stub.server.createContext("/", stub::handleUnknown);
    stub.server.start();
    return stub;
  }

  /** Runs the stub standalone, e.g. as an e2e compose service. */
  public static void main(String[] args) throws IOException {
    var bind = System.getenv().getOrDefault("STUB_BIND", "127.0.0.1");
    var port = Integer.parseInt(System.getenv().getOrDefault("STUB_PORT", "8080"));
    var stub = start(new InetSocketAddress(bind, port));
    System.out.printf("eventsv2 stub listening on %s:%d%n", bind, stub.port());
  }

  public int port() {
    return server.getAddress().getPort();
  }

  public List<RecordedRequest> recordedRequests() {
    synchronized (requests) {
      return List.copyOf(requests);
    }
  }

  public void stop() {
    server.stop(0);
  }

  @SuppressWarnings("unchecked")
  private void handlePutRawEvents(HttpExchange exchange) throws IOException {
    try (exchange) {
      if (!"POST".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      var body = (Map<String, Object>) Cbor.decode(exchange.getRequestBody().readAllBytes());
      var entries = new ArrayList<RecordedEntry>();
      var results = new ArrayList<Map<String, Object>>();
      for (var item : (List<Object>) body.getOrDefault("Entries", List.of())) {
        var entry = (Map<String, Object>) item;
        entries.add(
            new RecordedEntry(
                (byte[]) entry.get("Data"),
                stringMap(entry.get("Metadata")),
                stringMap(entry.get("SystemMetadata"))));
        results.add(Map.of("EventId", UUID.randomUUID().toString(), "SuccessCode", "PUBLISHED"));
      }
      requests.add(new RecordedRequest(String.valueOf(body.get("EventBusArn")), entries));

      var response = Cbor.encode(Map.of("Entries", results, "FailedEntryCount", 0));
      exchange.getResponseHeaders().add("Content-Type", "application/cbor");
      exchange.getResponseHeaders().add("smithy-protocol", "rpc-v2-cbor");
      exchange.sendResponseHeaders(200, response.length);
      exchange.getResponseBody().write(response);
    }
  }

  private void handleListRequests(HttpExchange exchange) throws IOException {
    try (exchange) {
      var json = new StringBuilder("[");
      var snapshot = recordedRequests();
      for (int i = 0; i < snapshot.size(); i++) {
        var request = snapshot.get(i);
        json.append(i == 0 ? "" : ",")
            .append("{\"eventBusArn\":")
            .append(quote(request.eventBusArn()))
            .append(",\"entries\":[");
        for (int j = 0; j < request.entries().size(); j++) {
          var entry = request.entries().get(j);
          json.append(j == 0 ? "" : ",")
              .append("{\"data\":")
              .append(quote(Base64.getEncoder().encodeToString(entry.data())))
              .append(",\"metadata\":")
              .append(jsonObject(entry.metadata()))
              .append(",\"systemMetadata\":")
              .append(jsonObject(entry.systemMetadata()))
              .append("}");
        }
        json.append("]}");
      }
      var bytes = json.append("]").toString().getBytes(UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, bytes.length);
      exchange.getResponseBody().write(bytes);
    }
  }

  private void handleHealth(HttpExchange exchange) throws IOException {
    try (exchange) {
      exchange.sendResponseHeaders(200, -1);
    }
  }

  private void handleUnknown(HttpExchange exchange) throws IOException {
    try (exchange) {
      exchange.sendResponseHeaders(404, -1);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> stringMap(Object value) {
    var map = new LinkedHashMap<String, String>();
    if (value instanceof Map<?, ?> m) {
      ((Map<String, Object>) m).forEach((k, v) -> map.put(k, String.valueOf(v)));
    }
    return map;
  }

  private static String jsonObject(Map<String, String> map) {
    var json = new StringBuilder("{");
    var first = true;
    for (var e : map.entrySet()) {
      json.append(first ? "" : ",")
          .append(quote(e.getKey()))
          .append(":")
          .append(quote(e.getValue()));
      first = false;
    }
    return json.append("}").toString();
  }

  private static String quote(String s) {
    var json = new StringBuilder("\"");
    for (char c : s.toCharArray()) {
      switch (c) {
        case '"' -> json.append("\\\"");
        case '\\' -> json.append("\\\\");
        default -> {
          if (c < 0x20) {
            json.append(String.format("\\u%04x", (int) c));
          } else {
            json.append(c);
          }
        }
      }
    }
    return json.append('"').toString();
  }
}
