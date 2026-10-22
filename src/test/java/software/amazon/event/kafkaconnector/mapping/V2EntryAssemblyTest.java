/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class V2EntryAssemblyTest {

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
  void provenanceContainsTopicPartitionOffsetAndStringKey() {
    var record = new SinkRecord("orders", 2, STRING_SCHEMA, "order-1001", STRING_SCHEMA, "{}", 41L);

    var metadata = V2EntryAssembly.provenance(record);

    assertThat(metadata)
        .containsEntry("kafka.topic", "orders")
        .containsEntry("kafka.partition", "2")
        .containsEntry("kafka.offset", "41")
        .containsEntry("kafka.key", "order-1001");
  }

  @Test
  void provenanceEncodesBinaryKeyAsBase64() {
    var record =
        new SinkRecord("orders", 0, null, new byte[] {0x01, 0x02}, STRING_SCHEMA, "{}", 7L);

    var metadata = V2EntryAssembly.provenance(record);

    assertThat(metadata).containsEntry("kafka.key", "AQI=");
  }

  @Test
  void provenanceOmitsMissingKey() {
    var record = new SinkRecord("orders", 0, null, null, STRING_SCHEMA, "{}", 7L);

    var metadata = V2EntryAssembly.provenance(record);

    assertThat(metadata).doesNotContainKey("kafka.key");
  }

  @Test
  void eventGroupIdDefaultsToTopicPartition() {
    var record = new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L);
    var config = configWith(Map.of());

    assertThat(V2EntryAssembly.eventGroupId(config, record)).isEqualTo("orders-2");
  }

  @Test
  void eventGroupIdNoneDisablesGrouping() {
    var record = new SinkRecord("orders", 2, STRING_SCHEMA, "order-1", STRING_SCHEMA, "{}", 41L);
    var config = configWith(Map.of("aws.eventbridge.v2.event.group.id", "none"));

    assertThat(V2EntryAssembly.eventGroupId(config, record)).isNull();
  }

  @Test
  void eventGroupIdRecordKeyUsesKey() {
    var record = new SinkRecord("orders", 2, STRING_SCHEMA, "order-1001", STRING_SCHEMA, "{}", 41L);
    var config =
        configWith(
            Map.of(
                "aws.eventbridge.v2.event.group.id",
                "record-key",
                "aws.eventbridge.v2.event.group.id.record.key.ack",
                true));

    assertThat(V2EntryAssembly.eventGroupId(config, record)).isEqualTo("order-1001");
  }

  @Test
  void eventGroupIdRecordKeyFallsBackToTopicPartitionForKeylessRecords() {
    var record = new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L);
    var config =
        configWith(
            Map.of(
                "aws.eventbridge.v2.event.group.id",
                "record-key",
                "aws.eventbridge.v2.event.group.id.record.key.ack",
                true));

    assertThat(V2EntryAssembly.eventGroupId(config, record)).isEqualTo("orders-2");
  }

  @Test
  void deduplicationIdDefaultsToTopicPartitionOffset() {
    var record = new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L);
    var config = configWith(Map.of());

    assertThat(V2EntryAssembly.deduplicationId(config, record)).isEqualTo("orders-2-41");
  }

  @Test
  void deduplicationIdNoneDisablesDeduplication() {
    var record = new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L);
    var config = configWith(Map.of("aws.eventbridge.v2.deduplication.id", "none"));

    assertThat(V2EntryAssembly.deduplicationId(config, record)).isNull();
  }
}
