/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;
import software.amazon.event.kafkaconnector.TestUtils.ListAppender;
import software.amazon.event.kafkaconnector.util.PublishCounters;

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
  void eventGroupIdRecordKeyTreatsEmptyStringKeyAsMissing() {
    var record = new SinkRecord("orders", 2, STRING_SCHEMA, "", STRING_SCHEMA, "{}", 41L);

    assertThat(V2EntryAssembly.eventGroupId(recordKeyConfig(), record)).isEqualTo("orders-2");
  }

  @Test
  void eventGroupIdRecordKeyTreatsEmptyByteKeyAsMissing() {
    var record = new SinkRecord("orders", 2, null, new byte[0], STRING_SCHEMA, "{}", 41L);

    assertThat(V2EntryAssembly.eventGroupId(recordKeyConfig(), record)).isEqualTo("orders-2");
  }

  @Test
  void provenanceOmitsEmptyStringKey() {
    var record = new SinkRecord("orders", 0, STRING_SCHEMA, "", STRING_SCHEMA, "{}", 7L);

    assertThat(V2EntryAssembly.provenance(record)).doesNotContainKey("kafka.key");
  }

  @Test
  void provenanceOmitsEmptyByteKey() {
    var record = new SinkRecord("orders", 0, null, new byte[0], STRING_SCHEMA, "{}", 7L);

    assertThat(V2EntryAssembly.provenance(record)).doesNotContainKey("kafka.key");
  }

  private EventBridgeSinkConfig recordKeyConfig() {
    return recordKeyConfig(Map.of());
  }

  private EventBridgeSinkConfig recordKeyConfig(Map<String, Object> extra) {
    var props = new HashMap<String, Object>(extra);
    props.put("aws.eventbridge.v2.event.group.id", "record-key");
    props.put("aws.eventbridge.v2.event.group.id.record.key.ack", true);
    return configWith(props);
  }

  @Test
  void missingKeyWithDefaultPolicyFallsBackAndIsCounted() {
    var counters = new PublishCounters();
    var assembly = new V2EntryAssembly(recordKeyConfig(), counters);
    var record = new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L);

    var result = assembly.eventGroup(record);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.success().getValue()).isEqualTo("orders-2");
    assertThat(counters.keyFallbacks()).isEqualTo(1);
  }

  @Test
  void fallbackWarnsOncePerTopicPartition() {
    var appender = ListAppender.of(V2EntryAssembly.class, Level.WARN);
    appender.start();
    try {
      var counters = new PublishCounters();
      var assembly = new V2EntryAssembly(recordKeyConfig(), counters);

      assembly.eventGroup(new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L));
      assembly.eventGroup(new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 42L));
      assembly.eventGroup(new SinkRecord("orders", 3, null, null, STRING_SCHEMA, "{}", 7L));

      var warnings =
          appender.getLoggingEvents().stream()
              .filter(e -> e.getLevel() == Level.WARN)
              .map(ILoggingEvent::getFormattedMessage)
              .toList();
      assertThat(warnings).hasSize(2);
      assertThat(warnings.get(0))
          .contains("orders")
          .contains("partition=2")
          .contains("offset=41")
          .contains("aws.eventbridge.v2.event.group.id.missing.key");
      assertThat(warnings.get(1)).contains("partition=3").contains("offset=7");
      assertThat(counters.keyFallbacks()).isEqualTo(3);
    } finally {
      appender.detach();
    }
  }

  @Test
  void missingKeyWithReportPolicyIsReportOnlyFailure() {
    var counters = new PublishCounters();
    var config = recordKeyConfig(Map.of("aws.eventbridge.v2.event.group.id.missing.key", "report"));
    var assembly = new V2EntryAssembly(config, counters);
    var nullKey = new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L);
    var emptyKey = new SinkRecord("orders", 2, STRING_SCHEMA, "", STRING_SCHEMA, "{}", 42L);

    for (var record : java.util.List.of(nullKey, emptyKey)) {
      var result = assembly.eventGroup(record);

      assertThat(result.isFailure()).isTrue();
      var error = result.failure().getValue();
      assertThat(error.getType()).isEqualTo(EventBridgeResult.ErrorType.REPORT_ONLY);
      assertThat(error.getMessage())
          .contains("no key")
          .contains("aws.eventbridge.v2.event.group.id.missing.key");
    }
    assertThat(counters.keyFallbacks()).isZero();
  }

  @Test
  void missingKeyWithFailPolicyPanicsNamingKeyAndSetting() {
    var config = recordKeyConfig(Map.of("aws.eventbridge.v2.event.group.id.missing.key", "fail"));
    var assembly = new V2EntryAssembly(config, new PublishCounters());
    var record = new SinkRecord("orders", 2, null, new byte[0], STRING_SCHEMA, "{}", 41L);

    var result = assembly.eventGroup(record);

    assertThat(result.isFailure()).isTrue();
    var error = result.failure().getValue();
    assertThat(error.getType()).isEqualTo(EventBridgeResult.ErrorType.PANIC);
    assertThat(error.getCause().getMessage())
        .contains("no key")
        .contains("aws.eventbridge.v2.event.group.id.missing.key")
        .contains("offset=41");
  }

  @Test
  void missingKeyPolicyHasNoEffectUnderTopicPartitionGrouping() {
    var counters = new PublishCounters();
    var config =
        configWith(
            Map.of(
                "aws.eventbridge.v2.event.group.id", "topic-partition",
                "aws.eventbridge.v2.event.group.id.missing.key", "report"));
    var record = new SinkRecord("orders", 2, null, null, STRING_SCHEMA, "{}", 41L);

    var result = new V2EntryAssembly(config, counters).eventGroup(record);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.success().getValue()).isEqualTo("orders-2");
    assertThat(counters.keyFallbacks()).isZero();
  }

  @Test
  void recordWithKeyIsUnaffectedByEveryPolicy() {
    var record = new SinkRecord("orders", 2, STRING_SCHEMA, "order-7", STRING_SCHEMA, "{}", 41L);

    for (var policy : java.util.List.of("fallback", "report", "fail")) {
      var counters = new PublishCounters();
      var config = recordKeyConfig(Map.of("aws.eventbridge.v2.event.group.id.missing.key", policy));

      var result = new V2EntryAssembly(config, counters).eventGroup(record);

      assertThat(result.isSuccess()).isTrue();
      assertThat(result.success().getValue()).isEqualTo("order-7");
      assertThat(counters.keyFallbacks()).isZero();
    }
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

  @Test
  void idsFromLongTopicStayWithinServiceLimit() {
    var topic = "t".repeat(200);
    var record = new SinkRecord(topic, 2, null, null, STRING_SCHEMA, "{}", 41L);
    var config = configWith(Map.of());

    var groupId = V2EntryAssembly.eventGroupId(config, record);
    var dedupId = V2EntryAssembly.deduplicationId(config, record);

    assertThat(groupId).hasSizeBetween(1, 128);
    assertThat(dedupId).hasSizeBetween(1, 128);
  }

  @Test
  void shortenedIdIsPrefixDashAndSha256OfFullValue() {
    var record = new SinkRecord("t".repeat(200), 2, null, null, STRING_SCHEMA, "{}", 41L);
    var config = configWith(Map.of());

    var groupId = V2EntryAssembly.eventGroupId(config, record);

    // pinned: changing the scheme changes every long-named group on the bus
    assertThat(groupId)
        .isEqualTo(
            "t".repeat(63) + "-e2f5dd03937f0b0f32af2fd10e19def8086276d723e3bc2d69e9b1965ebc1636")
        .hasSize(128);
  }

  @Test
  void shortenedIdIsDeterministic() {
    var value = "x".repeat(300);

    assertThat(V2EntryAssembly.shorten(value)).isEqualTo(V2EntryAssembly.shorten(value));
  }

  @Test
  void longValuesSharingPrefixShortenDifferently() {
    var prefix = "p".repeat(100);

    assertThat(V2EntryAssembly.shorten(prefix + "-a"))
        .isNotEqualTo(V2EntryAssembly.shorten(prefix + "-b"));
  }

  @Test
  void valueOfExactly128CharactersPassesThrough() {
    var value = "v".repeat(128);

    assertThat(V2EntryAssembly.shorten(value)).isEqualTo(value);
  }

  @Test
  void longRecordKeyIsShortenedUnderRecordKeyGrouping() {
    var key = "k".repeat(500);
    var first = new SinkRecord("orders", 2, STRING_SCHEMA, key, STRING_SCHEMA, "{}", 41L);
    var second = new SinkRecord("orders", 5, STRING_SCHEMA, key, STRING_SCHEMA, "{}", 42L);
    var config =
        configWith(
            Map.of(
                "aws.eventbridge.v2.event.group.id",
                "record-key",
                "aws.eventbridge.v2.event.group.id.record.key.ack",
                true));

    var firstId = V2EntryAssembly.eventGroupId(config, first);

    assertThat(firstId).hasSizeLessThanOrEqualTo(128);
    assertThat(V2EntryAssembly.eventGroupId(config, second)).isEqualTo(firstId);
  }

  @Test
  void shorteningDoesNotSplitSurrogatePairAtPrefixBoundary() {
    // character 63 (index 62) is the high surrogate of U+1F600
    var value = "a".repeat(62) + "\uD83D\uDE00" + "b".repeat(100);

    var shortened = V2EntryAssembly.shorten(value);

    assertThat(shortened).startsWith("a".repeat(62) + "-").hasSizeLessThanOrEqualTo(128);
    assertThat(Character.isHighSurrogate(shortened.charAt(61))).isFalse();
  }
}
