/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class SchemaRegistryConfigurationsTest {

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
  void glueRegistryIsNamedForAvro() {
    var glueArn = "arn:aws:glue:eu-central-1:123456789012:registry/orders";
    var config =
        configWith(
            Map.of(
                "aws.eventbridge.v2.content.type",
                "application/avro",
                "aws.eventbridge.v2.schema.registry.uri",
                glueArn));

    var registry = SchemaRegistryConfigurations.from(config);

    assertThat(registry).isPresent();
    assertThat(registry.get().registryUri()).isEqualTo(glueArn);
    assertThat(registry.get().confluentPublicRegistryConfiguration()).isNull();
  }

  @Test
  void confluentRegistryIsNamedWithConnectionForProtobuf() {
    var url = "https://psrc-example.eu-central-1.aws.confluent.cloud";
    var connectionArn =
        "arn:aws:events:eu-central-1:123456789012:connection/confluent-registry/abc";
    var config =
        configWith(
            Map.of(
                "aws.eventbridge.v2.content.type", "application/protobuf",
                "aws.eventbridge.v2.schema.registry.uri", url,
                "aws.eventbridge.v2.schema.registry.connection.arn", connectionArn));

    var registry = SchemaRegistryConfigurations.from(config);

    assertThat(registry).isPresent();
    assertThat(registry.get().registryUri()).isEqualTo(url);
    assertThat(registry.get().confluentPublicRegistryConfiguration().connectionArn())
        .isEqualTo(connectionArn);
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"application/json", "application/octet-stream"})
  void registryIsOmittedForNonOpenFormatContentTypes(String contentType) {
    var config = configWith(Map.of("aws.eventbridge.v2.content.type", contentType));

    assertThat(SchemaRegistryConfigurations.from(config)).isEmpty();
  }

  @Test
  void registryIsOmittedForCloudEventsStrategy() {
    var config = configWith(Map.of("aws.eventbridge.v2.payload.strategy", "cloudevents"));

    assertThat(SchemaRegistryConfigurations.from(config)).isEmpty();
  }
}
