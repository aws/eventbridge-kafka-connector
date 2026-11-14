/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

/**
 * Shared assembly of v2 entry metadata: record provenance (Metadata map), event grouping, and
 * deduplication identifiers derived from a Kafka record.
 */
public class V2EntryAssembly {

  static final int MAX_ID_LENGTH = 128;
  private static final int SHORTENED_PREFIX_LENGTH = 63;

  private V2EntryAssembly() {}

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
