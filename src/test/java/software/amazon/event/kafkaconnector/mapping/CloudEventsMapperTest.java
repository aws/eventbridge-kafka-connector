/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import io.cloudevents.CloudEvent;
import io.cloudevents.jackson.JsonFormat;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class CloudEventsMapperTest {

  private EventBridgeSinkConfig configWith(Map<String, Object> extra) {
    var props = new HashMap<String, Object>();
    props.put("aws.eventbridge.connector.id", "orders-connector");
    props.put("aws.eventbridge.region", "eu-central-1");
    props.put(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:eu-central-1:123456789012:event-busv2/my-bus/ABCDEFGHIJKLM1234567890");
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "cloudevents");
    props.putAll(extra);
    return new EventBridgeSinkConfig(props);
  }

  private static CloudEvent parse(PutRawEventsRequestEntry entry) {
    return new JsonFormat().deserialize(entry.data().asByteArray());
  }

  @Test
  void mapsRecordMetadataToCloudEventAttributesWithDefaultMappers() {
    var record = new SinkRecord("orders", 2, STRING_SCHEMA, "order-1001", null, "{\"a\":1}", 41L);
    var mapper = new CloudEventsMapper(configWith(Map.of()));

    var event = parse(mapper.map(record).success().getValue());

    assertThat(event.getSpecVersion().toString()).isEqualTo("1.0");
    assertThat(event.getSource()).isEqualTo(URI.create("kafka-connect.orders-connector"));
    assertThat(event.getType()).isEqualTo("kafka-connect-orders");
    assertThat(event.getId()).isEqualTo("orders-2-41");
    assertThat(event.getSubject()).isEqualTo("orders");
    assertThat(event.getTime()).isNull();
  }

  @Test
  void structValueBecomesJsonData() throws Exception {
    var schema =
        org.apache.kafka.connect.data.SchemaBuilder.struct()
            .field("orderId", STRING_SCHEMA)
            .build();
    var value = new org.apache.kafka.connect.data.Struct(schema).put("orderId", "1001");
    var record = new SinkRecord("orders", 0, null, null, schema, value, 1L);
    var mapper = new CloudEventsMapper(configWith(Map.of()));

    var entry = mapper.map(record).success().getValue();
    var event = parse(entry);

    assertThat(entry.systemMetadata().contentType()).isEqualTo("application/json");
    assertThat(event.getDataContentType()).isEqualTo("application/json");
    var data =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(event.getData().toBytes());
    assertThat(data.get("orderId").asText()).isEqualTo("1001");
  }

  @Test
  void binaryValueBecomesDataBase64WithOctetStreamContentType() throws Exception {
    var value = new byte[] {0x00, 0x01, (byte) 0xFF};
    var record = new SinkRecord("orders", 0, null, null, null, value, 1L);
    var mapper = new CloudEventsMapper(configWith(Map.of()));

    var entry = mapper.map(record).success().getValue();
    var envelope =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(entry.data().asByteArray());
    var event = parse(entry);

    assertThat(envelope.has("data")).isFalse();
    assertThat(envelope.get("data_base64").asText()).isEqualTo("AAH/");
    assertThat(event.getDataContentType()).isEqualTo("application/octet-stream");
    assertThat(event.getData().toBytes()).isEqualTo(value);
    assertThat(entry.systemMetadata().contentType()).isEqualTo("application/json");
  }

  @Test
  void tombstoneFailsReportOnly() {
    var record = new SinkRecord("orders", 0, null, null, null, null, 1L);
    var mapper = new CloudEventsMapper(configWith(Map.of()));

    var result = mapper.map(record);

    assertThat(result.isFailure()).isTrue();
    assertThat(result.failure().getValue().getType())
        .isEqualTo(software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.REPORT_ONLY);
    assertThat(result.failure().getValue().getMessage()).contains("null value");
  }

  @Test
  void appliesSharedProvenanceGroupAndDeduplicationMetadata() {
    var record = new SinkRecord("orders", 2, STRING_SCHEMA, "order-1001", null, "{}", 41L);
    var mapper = new CloudEventsMapper(configWith(Map.of()));

    var entry = mapper.map(record).success().getValue();

    assertThat(entry.systemMetadata().eventGroupId()).isEqualTo("orders-2");
    assertThat(entry.systemMetadata().deduplicationId()).isEqualTo("orders-2-41");
    assertThat(entry.metadata())
        .containsEntry("kafka.topic", "orders")
        .containsEntry("kafka.partition", "2")
        .containsEntry("kafka.offset", "41")
        .containsEntry("kafka.key", "order-1001");
  }

  @Test
  void usesConfiguredCustomDetailTypeAndTimeMappers() {
    var record = new SinkRecord("orders", 0, null, null, null, "{}", 1L);
    var mapper =
        new CloudEventsMapper(
            configWith(
                Map.of(
                    "aws.eventbridge.detail.types.mapper.class",
                    FixedDetailTypeMapper.class.getName(),
                    "aws.eventbridge.time.mapper.class",
                    FixedTimeMapper.class.getName())));

    var event = parse(mapper.map(record).success().getValue());

    assertThat(event.getType()).isEqualTo("com.example.order.created");
    assertThat(event.getTime().toInstant()).isEqualTo(FixedTimeMapper.TIME);
  }

  public static class FixedDetailTypeMapper implements DetailTypeMapper {
    @Override
    public String getDetailType(SinkRecord record) {
      return "com.example.order.created";
    }

    @Override
    public void configure(EventBridgeSinkConfig eventBridgeSinkConfig) {}
  }

  public static class FixedTimeMapper implements TimeMapper {
    static final java.time.Instant TIME = java.time.Instant.parse("2026-10-21T10:15:30Z");

    @Override
    public java.time.Instant getTime(SinkRecord sinkRecord) {
      return TIME;
    }
  }
}
