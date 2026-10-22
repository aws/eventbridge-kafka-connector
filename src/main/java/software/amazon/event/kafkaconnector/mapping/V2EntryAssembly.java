/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

/**
 * Shared assembly of v2 entry metadata: record provenance (Metadata map), event grouping, and
 * deduplication identifiers derived from a Kafka record.
 */
public class V2EntryAssembly {

  private V2EntryAssembly() {}

  /**
   * Provenance Metadata entries for a record: kafka.topic, kafka.partition, kafka.offset, and
   * kafka.key when the record has a key.
   */
  public static Map<String, String> provenance(SinkRecord record) {
    var metadata = new HashMap<String, String>();
    metadata.put("kafka.topic", record.topic());
    metadata.put("kafka.partition", String.valueOf(record.kafkaPartition()));
    metadata.put("kafka.offset", String.valueOf(record.kafkaOffset()));
    if (record.key() != null) {
      metadata.put("kafka.key", keyAsString(record.key()));
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
    if ("record-key".equals(config.v2EventGroupId) && record.key() != null) {
      return keyAsString(record.key());
    }
    return record.topic() + "-" + record.kafkaPartition();
  }

  /** DeduplicationId for a record per the configured strategy; null when disabled. */
  public static String deduplicationId(EventBridgeSinkConfig config, SinkRecord record) {
    if ("none".equals(config.v2DeduplicationId)) {
      return null;
    }
    return record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset();
  }
}
