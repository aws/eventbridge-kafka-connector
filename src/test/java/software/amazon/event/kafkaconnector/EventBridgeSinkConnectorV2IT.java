/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Base64;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * End-to-end v2 (Custom Event Bus) flow: Kafka -> Kafka Connect -> connector -> PutRawEvents ->
 * eventsv2 stub, using the raw payload strategy with ByteArrayConverter.
 */
@TestMethodOrder(OrderAnnotation.class)
public class EventBridgeSinkConnectorV2IT extends AbstractEventBridgeSinkConnectorIT {

  private static final String TOPIC = "eventbridge-e2e-v2";
  private static final String KEY = "order-1001";
  private static final String BUS_ARN =
      "arn:aws:events:us-east-1:000000000000:event-busv2/eventbridge-e2e/abcdefghijklmnopqrstuvwxy";

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  @Order(1)
  public void startConnector() throws IOException, InterruptedException {
    startConnector(Path.of("e2e/connect-config-v2.json"), "eventbridge-e2e-v2");
  }

  @Test
  @Order(2)
  public void sendJsonRecordToKafkaReceivePutRawEventsEntryAtStub() {
    var sentTimestamp = LocalDateTime.now().toString();
    var value =
        mapper
            .createObjectNode()
            .put("sentTimestamp", sentTimestamp)
            .put("message", "hello from kafka v2");

    sendKafkaRecord(new ProducerRecord<>(TOPIC, KEY, value));

    await()
        .atMost(15, SECONDS)
        .pollInterval(1, SECONDS)
        .untilAsserted(
            () -> {
              var requests = mapper.readTree(stubRecordedRequests());
              log.info("eventsv2 stub recorded requests: {}", requests);
              assertThat(requests).hasSize(1);

              var request = requests.get(0);
              assertThat(request.path("eventBusArn").asText()).isEqualTo(BUS_ARN);
              assertThat(request.path("entries")).hasSize(1);

              var entry = request.path("entries").get(0);
              var data = mapper.readTree(Base64.getDecoder().decode(entry.path("data").asText()));
              assertThat(data.path("sentTimestamp").asText()).isEqualTo(sentTimestamp);
              assertThat(data.path("message").asText()).isEqualTo("hello from kafka v2");

              var systemMetadata = entry.path("systemMetadata");
              assertThat(systemMetadata.path("ContentType").asText()).isEqualTo("application/json");
              assertThat(systemMetadata.path("EventGroupId").asText()).startsWith(TOPIC + "-");
              assertThat(systemMetadata.path("DeduplicationId").asText()).startsWith(TOPIC + "-");

              var metadata = entry.path("metadata");
              assertThat(metadata.path("kafka.topic").asText()).isEqualTo(TOPIC);
              assertThat(metadata.path("kafka.key").asText()).isEqualTo(KEY);
            });
  }

  @Test
  @Order(3)
  public void connectorTaskIsRunning() throws IOException, InterruptedException {
    assertThat(connectorStateOfTask(0)).isEqualTo("RUNNING");
  }

  private String stubRecordedRequests() throws IOException, InterruptedException {
    var request =
        HttpRequest.newBuilder()
            .uri(eventsV2StubUri("/_stub/requests"))
            .setHeader("Accept", "application/json")
            .GET()
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
  }
}
