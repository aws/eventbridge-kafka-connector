/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;
import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.AWS_V2_EVENT_GROUP_ID_MISSING_KEY_CONFIG;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.PublishCounters;

/**
 * Shared assembly of v2 entry metadata: record provenance (Metadata map), event grouping, and
 * deduplication identifiers derived from a Kafka record.
 */
public class V2EntryAssembly {

  static final int MAX_ID_LENGTH = 128;
  private static final int SHORTENED_PREFIX_LENGTH = 63;

  private final Logger log = ContextAwareLoggerFactory.getLogger(V2EntryAssembly.class);
  private final EventBridgeSinkConfig config;
  private final PublishCounters counters;
  private final Set<String> warnedPartitions = ConcurrentHashMap.newKeySet();

  /** Per-task assembly that applies the missing-key policy and counts key fallbacks. */
  public V2EntryAssembly(EventBridgeSinkConfig config, PublishCounters counters) {
    this.config = config;
    this.counters = counters;
  }

  /**
   * EventGroupId for a record, applying the missing-key policy under record-key grouping. The
   * success value is null when grouping is disabled.
   */
  public EventBridgeResult<String> eventGroup(SinkRecord record) {
    if ("record-key".equals(config.v2EventGroupId) && presentKey(record) == null) {
      if ("report".equals(config.v2EventGroupIdMissingKey)) {
        return failure(record, reportOnly(missingKeyMessage(record)));
      }
      if ("fail".equals(config.v2EventGroupIdMissingKey)) {
        return failure(record, panic(new DataException(missingKeyMessage(record))));
      }
      counters.addKeyFallbacks(1);
      if (warnedPartitions.add(record.topic() + "-" + record.kafkaPartition())) {
        log.warn(
            "Record has no key under record-key grouping; grouping by topic-partition instead "
                + "(logged once per topic-partition; see \"{}\"): topic={} partition={} offset={}",
            AWS_V2_EVENT_GROUP_ID_MISSING_KEY_CONFIG,
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset());
      }
    }
    return success(record, eventGroupId(config, record));
  }

  private String missingKeyMessage(SinkRecord record) {
    return String.format(
        "Record has no key under record-key grouping and \"%s\" is \"%s\": topic=%s partition=%s"
            + " offset=%s",
        AWS_V2_EVENT_GROUP_ID_MISSING_KEY_CONFIG,
        config.v2EventGroupIdMissingKey,
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset());
  }

  /**
   * Provenance Metadata entries for a record: kafka.topic, kafka.partition, kafka.offset, and
   * kafka.key when the record has a key with a non-empty string form (metadata values cannot be
   * empty).
   */
  public static Map<String, String> provenance(SinkRecord record) {
    var metadata = new HashMap<String, String>();
    metadata.put("kafka.topic", record.topic());
    metadata.put("kafka.partition", String.valueOf(record.kafkaPartition()));
    metadata.put("kafka.offset", String.valueOf(record.kafkaOffset()));
    var key = presentKey(record);
    if (key != null) {
      metadata.put("kafka.key", key);
    }
    return metadata;
  }

  static String keyAsString(Object key) {
    if (key instanceof byte[] bytes) {
      return java.util.Base64.getEncoder().encodeToString(bytes);
    }
    return key.toString();
  }

  /** EventGroupId for a record per the configured strategy; null when grouping is disabled. */
  public static String eventGroupId(EventBridgeSinkConfig config, SinkRecord record) {
    if ("none".equals(config.v2EventGroupId)) {
      return null;
    }
    if ("record-key".equals(config.v2EventGroupId)) {
      var key = presentKey(record);
      if (key != null) {
        return shorten(key);
      }
    }
    return shorten(record.topic() + "-" + record.kafkaPartition());
  }

  /** The key's string form, or null when the key is missing: null or with an empty string form. */
  static String presentKey(SinkRecord record) {
    if (record.key() == null) {
      return null;
    }
    var key = keyAsString(record.key());
    return key.isEmpty() ? null : key;
  }

  /** DeduplicationId for a record per the configured strategy; null when disabled. */
  public static String deduplicationId(EventBridgeSinkConfig config, SinkRecord record) {
    if ("none".equals(config.v2DeduplicationId)) {
      return null;
    }
    return shorten(record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset());
  }

  /**
   * Returns the value unchanged when it fits the service's 128-character identifier limit;
   * otherwise its first 63 characters, a dash, and the lowercase hex SHA-256 of the full value.
   */
  static String shorten(String value) {
    if (value.length() <= MAX_ID_LENGTH) {
      return value;
    }
    var prefixLength = SHORTENED_PREFIX_LENGTH;
    if (Character.isHighSurrogate(value.charAt(prefixLength - 1))) {
      prefixLength--; // keep the surrogate pair whole
    }
    return value.substring(0, prefixLength) + "-" + sha256Hex(value);
  }

  private static String sha256Hex(String value) {
    try {
      var digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }
}
