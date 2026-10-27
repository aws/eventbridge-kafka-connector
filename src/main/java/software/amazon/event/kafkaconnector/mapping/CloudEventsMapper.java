/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;

import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import java.net.URI;
import java.time.ZoneOffset;
import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

/** Maps Kafka records to v2 entries carrying a CloudEvents 1.0 structured JSON envelope. */
public class CloudEventsMapper {

  private static final String SOURCE_PREFIX = "kafka-connect.";

  private final EventBridgeSinkConfig config;
  private final DetailTypeMapper detailTypeMapper;
  private final TimeMapper timeMapper;
  private final JsonFormat jsonFormat = new JsonFormat();
  // same conversion semantics as the v1 envelope value (SinkRecordJsonMapper)
  private final JsonConverter jsonConverter = new JsonConverter();

  public CloudEventsMapper(EventBridgeSinkConfig config) {
    this.config = config;
    this.detailTypeMapper = MapperLoader.detailTypeMapper(config);
    this.timeMapper = MapperLoader.timeMapper(config);
    jsonConverter.configure(Map.of("schemas.enable", "false"), false);
  }

  public EventBridgeResult<PutRawEventsRequestEntry> map(SinkRecord record) {
    var value = record.value();
    if (value == null) {
      return failure(record, reportOnly(RawPayloadMapper.TOMBSTONE_MESSAGE));
    }

    var builder =
        CloudEventBuilder.v1()
            .withId(record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset())
            .withSource(URI.create(SOURCE_PREFIX + config.connectorId))
            .withType(detailTypeMapper.getDetailType(record))
            .withSubject(record.topic());

    var time = timeMapper.getTime(record);
    if (time != null) {
      builder.withTime(time.atOffset(ZoneOffset.UTC));
    }

    if (value instanceof byte[] binary) {
      // binary values are carried as data_base64 per the CloudEvents JSON format
      builder.withDataContentType("application/octet-stream").withData(binary);
    } else {
      var json = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), value);
      builder.withDataContentType("application/json").withData(json);
    }

    return success(
        record,
        PutRawEventsRequestEntry.builder()
            .data(SdkBytes.fromByteArray(jsonFormat.serialize(builder.build())))
            .metadata(V2EntryAssembly.provenance(record))
            .systemMetadata(
                sm ->
                    sm.contentType("application/json")
                        .eventGroupId(V2EntryAssembly.eventGroupId(config, record))
                        .deduplicationId(V2EntryAssembly.deduplicationId(config, record)))
            .build());
  }
}
