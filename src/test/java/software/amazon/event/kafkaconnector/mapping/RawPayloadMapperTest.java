/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class RawPayloadMapperTest {

  private EventBridgeSinkConfig configWith(Map<String, Object> extra) {
    var props = new HashMap<String, Object>();
    props.put("aws.eventbridge.connector.id", "someId");
    props.put("aws.eventbridge.region", "eu-central-1");
    props.put(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:eu-central-1:123456789012:event-busv2/my-bus/ABCDEFGHIJKLM1234567890");
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.putAll(extra);
    return new EventBridgeSinkConfig(props);
  }

  @Test
  void forwardsBinaryValueUnchanged() {
    var value = new byte[] {0x00, 0x01, (byte) 0xFF, 0x7F};
    var record = new SinkRecord("orders", 0, null, null, null, value, 1L);
    var mapper = new RawPayloadMapper(configWith(Map.of()));

    var result = mapper.map(record);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.success().getValue().data().asByteArray()).isEqualTo(value);
  }

  @Test
  void forwardsStringValueAsUtf8WithoutEnvelope() {
    var value = "{\"orderId\":\"1001\",\"note\":\"größe\"}";
    var record = new SinkRecord("orders", 0, null, null, null, value, 1L);
    var mapper = new RawPayloadMapper(configWith(Map.of()));

    var result = mapper.map(record);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.success().getValue().data().asByteArray())
        .isEqualTo(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  @Test
  void unsupportedValueTypeFailsReportOnlyNamingByteArrayConverter() {
    var schema =
        org.apache.kafka.connect.data.SchemaBuilder.struct()
            .field("orderId", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
    var value = new org.apache.kafka.connect.data.Struct(schema).put("orderId", "1001");
    var record = new SinkRecord("orders", 0, null, null, schema, value, 1L);
    var mapper = new RawPayloadMapper(configWith(Map.of()));

    var result = mapper.map(record);

    assertThat(result.isFailure()).isTrue();
    var error = result.failure().getValue();
    assertThat(error.getType())
        .isEqualTo(software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.REPORT_ONLY);
    assertThat(error.getMessage()).contains("ByteArrayConverter");
  }

  @Test
  void tombstoneFailsReportOnlyNamingNullValue() {
    var record = new SinkRecord("orders", 0, null, null, null, null, 1L);
    var mapper = new RawPayloadMapper(configWith(Map.of()));

    var result = mapper.map(record);

    assertThat(result.isFailure()).isTrue();
    var error = result.failure().getValue();
    assertThat(error.getType())
        .isEqualTo(software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.REPORT_ONLY);
    assertThat(error.getMessage()).contains("null value");
  }

  @Test
  void appliesConfiguredContentTypeAndSharedEntryMetadata() {
    var record =
        new SinkRecord(
            "orders",
            2,
            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
            "order-1001",
            null,
            new byte[] {0x01},
            41L);
    var mapper =
        new RawPayloadMapper(
            configWith(
                Map.of(
                    "aws.eventbridge.v2.content.type",
                    "application/avro",
                    "aws.eventbridge.v2.schema.registry.uri",
                    "arn:aws:glue:eu-central-1:123456789012:registry/orders")));

    var entry = mapper.map(record).success().getValue();

    assertThat(entry.systemMetadata().contentType()).isEqualTo("application/avro");
    assertThat(entry.systemMetadata().eventGroupId()).isEqualTo("orders-2");
    assertThat(entry.systemMetadata().deduplicationId()).isEqualTo("orders-2-41");
    assertThat(entry.metadata())
        .containsEntry("kafka.topic", "orders")
        .containsEntry("kafka.partition", "2")
        .containsEntry("kafka.offset", "41")
        .containsEntry("kafka.key", "order-1001");
  }

  @Test
  void defaultContentTypeIsOctetStream() {
    var record = new SinkRecord("orders", 0, null, null, null, new byte[] {0x01}, 1L);
    var mapper = new RawPayloadMapper(configWith(Map.of()));

    var entry = mapper.map(record).success().getValue();

    assertThat(entry.systemMetadata().contentType()).isEqualTo("application/octet-stream");
  }
}
