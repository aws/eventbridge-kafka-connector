/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.stub;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;
import software.amazon.event.kafkaconnector.EventBridgeV2ClientFactoryTest;
import software.amazon.event.kafkaconnector.TestV2Writers;

/**
 * Drives the real v2 writer and AWS SDK (RPC v2 CBOR over HTTP, SigV4) against the in-process
 * eventsv2 stub.
 */
public class EventsV2StubTest {

  private static final String BUS_ARN =
      "arn:aws:events:us-east-1:000000000000:event-busv2/eventbridge-e2e/abcdefghijklmnopqrstuvwxy";

  private EventsV2Stub stub;

  @BeforeEach
  void startStub() throws Exception {
    stub = EventsV2Stub.start(new InetSocketAddress("127.0.0.1", 0));
  }

  @AfterEach
  void stopStub() {
    stub.stop();
  }

  @Test
  void realWriterPublishesRawEntryThroughSdkToStub() {
    var props = new HashMap<String, Object>();
    props.put("aws.eventbridge.connector.id", "stub-test");
    props.put("aws.eventbridge.region", "us-east-1");
    props.put("aws.eventbridge.eventbus.arn", BUS_ARN);
    props.put("aws.eventbridge.endpoint.uri", "http://127.0.0.1:" + stub.port());
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.put("aws.eventbridge.v2.content.type", "application/json");
    props.put(
        "aws.eventbridge.auth.credentials_provider.class",
        EventBridgeV2ClientFactoryTest.StaticCredentialsProvider.class.getName());
    var config = new EventBridgeSinkConfig(props);
    var payload = "{\"orderId\":\"1001\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    var record = new SinkRecord("orders", 2, null, "order-1001", null, payload, 41L);

    var writer = TestV2Writers.create(config);
    try {
      var results = writer.putItems(List.of(record));
      assertThat(results).hasSize(1);
      assertThat(results.get(0).isSuccess()).isTrue();
    } finally {
      writer.shutdown();
    }

    var requests = stub.recordedRequests();
    assertThat(requests).hasSize(1);
    var request = requests.get(0);
    assertThat(request.eventBusArn()).isEqualTo(BUS_ARN);
    assertThat(request.entries()).hasSize(1);
    var entry = request.entries().get(0);
    assertThat(entry.data()).isEqualTo(payload);
    assertThat(entry.systemMetadata())
        .containsEntry("ContentType", "application/json")
        .containsEntry("EventGroupId", "orders-2")
        .containsEntry("DeduplicationId", "orders-2-41");
    assertThat(entry.metadata())
        .containsEntry("kafka.topic", "orders")
        .containsEntry("kafka.partition", "2")
        .containsEntry("kafka.offset", "41")
        .containsEntry("kafka.key", "order-1001");
  }
}
