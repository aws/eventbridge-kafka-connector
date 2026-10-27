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
 * End-to-end v2 (Custom Event Bus) flow with the CloudEvents payload strategy: Kafka -> Kafka
 * Connect (JsonConverter) -> connector -> PutRawEvents -> eventsv2 stub.
 */
@TestMethodOrder(OrderAnnotation.class)
public class EventBridgeSinkConnectorV2CloudEventsIT extends AbstractEventBridgeSinkConnectorIT {

  private static final String TOPIC = "eventbridge-e2e-v2-cloudevents";
  private static final String KEY = "order-2002";

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  @Order(1)
  public void startConnector() throws IOException, InterruptedException {
    startConnector(
        Path.of("e2e/connect-config-v2-cloudevents.json"), "eventbridge-e2e-v2-cloudevents");
  }

  @Test
  @Order(2)
  public void sendJsonRecordToKafkaReceiveCloudEventAtStub() {
    var sentTimestamp = LocalDateTime.now().toString();
    var value =
        mapper
            .createObjectNode()
            .put("sentTimestamp", sentTimestamp)
            .put("message", "hello from kafka cloudevents");

    sendKafkaRecord(new ProducerRecord<>(TOPIC, KEY, value));

    await()
        .atMost(15, SECONDS)
        .pollInterval(1, SECONDS)
        .untilAsserted(
            () -> {
              var requests = mapper.readTree(stubRecordedRequests());
              log.info("eventsv2 stub recorded requests: {}", requests);
              assertThat(requests).hasSize(1);

              var entry = requests.get(0).path("entries").get(0);
              assertThat(entry.path("systemMetadata").path("ContentType").asText())
                  .isEqualTo("application/json");

              var event = mapper.readTree(Base64.getDecoder().decode(entry.path("data").asText()));
              assertThat(event.path("specversion").asText()).isEqualTo("1.0");
              assertThat(event.path("source").asText())
                  .isEqualTo("kafka-connect.eventbridge-e2e-v2-cloudevents-connector");
              assertThat(event.path("type").asText()).isEqualTo("kafka-connect-" + TOPIC);
              assertThat(event.path("subject").asText()).isEqualTo(TOPIC);
              assertThat(event.path("id").asText()).startsWith(TOPIC + "-");
              assertThat(event.path("datacontenttype").asText()).isEqualTo("application/json");
              assertThat(event.path("data").path("sentTimestamp").asText())
                  .isEqualTo(sentTimestamp);
              assertThat(event.path("data").path("message").asText())
                  .isEqualTo("hello from kafka cloudevents");

              assertThat(entry.path("metadata").path("kafka.key").asText()).isEqualTo(KEY);
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
