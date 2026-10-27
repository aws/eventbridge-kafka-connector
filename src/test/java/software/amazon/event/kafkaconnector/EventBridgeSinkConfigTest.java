/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class EventBridgeSinkConfigTest {

  private Map<String, Object> baseProps() {
    var props = new HashMap<String, Object>();
    props.put("aws.eventbridge.connector.id", "someId");
    props.put("aws.eventbridge.region", "eu-central-1");
    props.put("aws.eventbridge.eventbus.arn", "something");
    return props;
  }

  @Test
  void apiVersionDefaultsToV1() {
    var config = new EventBridgeSinkConfig(baseProps());

    assertThat(config.apiVersion).isEqualTo("v1");
  }

  @Test
  void v2PayloadStrategyIsUnsetByDefaultAndParsesConfiguredValue() {
    var defaultConfig = new EventBridgeSinkConfig(baseProps());
    assertThat(defaultConfig.v2PayloadStrategy).isNull();

    var props = baseProps();
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "cloudevents");
    var config = new EventBridgeSinkConfig(props);
    assertThat(config.v2PayloadStrategy).isEqualTo("cloudevents");
  }

  @Test
  void v2ContentTypeDefaultsToOctetStreamAndParsesConfiguredValue() {
    var defaultConfig = new EventBridgeSinkConfig(baseProps());
    assertThat(defaultConfig.v2ContentType).isEqualTo("application/octet-stream");

    var props = baseProps();
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.put("aws.eventbridge.v2.content.type", "application/avro");
    var config = new EventBridgeSinkConfig(props);
    assertThat(config.v2ContentType).isEqualTo("application/avro");
  }

  @Test
  void v2SchemaRegistryKeysDefaultToEmptyAndParseConfiguredValues() {
    var defaultConfig = new EventBridgeSinkConfig(baseProps());
    assertThat(defaultConfig.v2SchemaRegistryUri).isEmpty();
    assertThat(defaultConfig.v2SchemaRegistryConnectionArn).isEmpty();

    var props = baseProps();
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.put("aws.eventbridge.v2.content.type", "application/protobuf");
    props.put("aws.eventbridge.v2.schema.registry.uri", "https://registry.example.com");
    props.put(
        "aws.eventbridge.v2.schema.registry.connection.arn",
        "arn:aws:events:eu-central-1:123456789012:connection/confluent-registry/abc");
    var config = new EventBridgeSinkConfig(props);
    assertThat(config.v2SchemaRegistryUri).isEqualTo("https://registry.example.com");
    assertThat(config.v2SchemaRegistryConnectionArn)
        .isEqualTo("arn:aws:events:eu-central-1:123456789012:connection/confluent-registry/abc");
  }

  @Test
  void v2EventGroupingDefaultsAndParsesConfiguredValues() {
    var defaultConfig = new EventBridgeSinkConfig(baseProps());
    assertThat(defaultConfig.v2EventGroupId).isEqualTo("topic-partition");
    assertThat(defaultConfig.v2EventGroupIdRecordKeyAck).isFalse();

    var props = baseProps();
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.put("aws.eventbridge.v2.event.group.id", "record-key");
    props.put("aws.eventbridge.v2.event.group.id.record.key.ack", true);
    var config = new EventBridgeSinkConfig(props);
    assertThat(config.v2EventGroupId).isEqualTo("record-key");
    assertThat(config.v2EventGroupIdRecordKeyAck).isTrue();
  }

  @Test
  void v2DeduplicationIdDefaultsToTopicPartitionOffsetAndParsesConfiguredValue() {
    var defaultConfig = new EventBridgeSinkConfig(baseProps());
    assertThat(defaultConfig.v2DeduplicationId).isEqualTo("topic-partition-offset");

    var props = baseProps();
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.put("aws.eventbridge.v2.deduplication.id", "none");
    var config = new EventBridgeSinkConfig(props);
    assertThat(config.v2DeduplicationId).isEqualTo("none");
  }

  @Test
  void readmeV2RawPassThroughExampleParsesAndValidates() {
    // mirrors the "Example: raw pass-through to a Custom Event Bus" connector config in README.md
    var props = new HashMap<String, Object>();
    props.put("auto.offset.reset", "earliest");
    props.put("connector.class", "software.amazon.event.kafkaconnector.EventBridgeSinkConnector");
    props.put("topics", "orders");
    props.put("aws.eventbridge.connector.id", "orders-v2-connector");
    props.put(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:us-east-1:123456789012:event-busv2/orders/EXAMPLE1234567890abcdef");
    props.put("aws.eventbridge.region", "us-east-1");
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.put("aws.eventbridge.v2.content.type", "application/avro");
    props.put(
        "aws.eventbridge.v2.schema.registry.uri",
        "arn:aws:glue:us-east-1:123456789012:registry/orders");
    props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    props.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");

    var config = new EventBridgeSinkConfig(props);
    assertThat(config.apiVersion).isEqualTo("v2");
    assertThat(config.v2PayloadStrategy).isEqualTo("raw");
    assertThat(config.v2ContentType).isEqualTo("application/avro");

    EventBridgeSinkConfigValidator.validateCrossField(props);
  }

  @Test
  void e2eV2ConnectorConfigParsesAndValidates() throws Exception {
    var root =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(java.nio.file.Path.of("e2e/connect-config-v2.json").toFile());
    var props = new HashMap<String, Object>();
    root.get("config").fields().forEachRemaining(e -> props.put(e.getKey(), e.getValue().asText()));

    var config = new EventBridgeSinkConfig(props);
    assertThat(config.apiVersion).isEqualTo("v2");
    assertThat(props.get("value.converter"))
        .isEqualTo("org.apache.kafka.connect.converters.ByteArrayConverter");

    EventBridgeSinkConfigValidator.validateCrossField(props);
  }
}
