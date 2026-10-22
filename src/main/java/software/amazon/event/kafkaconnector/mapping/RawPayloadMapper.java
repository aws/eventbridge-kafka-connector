/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;

import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

/**
 * Maps Kafka records to v2 entries by forwarding the record value bytes unchanged with the
 * configured static content type. Requires a byte-preserving converter (byte[] or String values).
 */
public class RawPayloadMapper {

  static final String TOMBSTONE_MESSAGE =
      "Record has a null value (tombstone); v2 mode has no payload to publish";

  private final EventBridgeSinkConfig config;

  public RawPayloadMapper(EventBridgeSinkConfig config) {
    this.config = config;
  }

  public EventBridgeResult<PutRawEventsRequestEntry> map(SinkRecord record) {
    var value = record.value();
    if (value == null) {
      return failure(record, reportOnly(TOMBSTONE_MESSAGE));
    }

    byte[] bytes;
    if (value instanceof byte[] b) {
      bytes = b;
    } else if (value instanceof String s) {
      bytes = s.getBytes(UTF_8);
    } else {
      return failure(record, reportOnly(unsupportedTypeMessage(value)));
    }

    return success(
        record,
        PutRawEventsRequestEntry.builder()
            .data(SdkBytes.fromByteArray(bytes))
            .metadata(V2EntryAssembly.provenance(record))
            .systemMetadata(
                sm ->
                    sm.contentType(config.v2ContentType)
                        .eventGroupId(V2EntryAssembly.eventGroupId(config, record))
                        .deduplicationId(V2EntryAssembly.deduplicationId(config, record)))
            .build());
  }

  private static String unsupportedTypeMessage(Object value) {
    return String.format(
        "Raw payload strategy requires byte[] or String record values but got %s; "
            + "configure a byte-preserving converter such as "
            + "org.apache.kafka.connect.converters.ByteArrayConverter",
        value.getClass().getName());
  }
}
