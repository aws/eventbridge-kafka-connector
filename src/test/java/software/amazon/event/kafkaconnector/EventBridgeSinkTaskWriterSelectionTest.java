/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.util.PublishCounters;

public class EventBridgeSinkTaskWriterSelectionTest {

  private static Map<String, String> baseProps() {
    var props = new HashMap<String, String>();
    props.put("aws.eventbridge.connector.id", "someId");
    props.put("aws.eventbridge.region", "eu-central-1");
    // explicit static provider so tests do not depend on ambient AWS credentials
    props.put(
        "aws.eventbridge.auth.credentials_provider.class",
        EventBridgeV2ClientFactoryTest.StaticCredentialsProvider.class.getName());
    return props;
  }

  @Test
  void defaultConfigurationSelectsClassicWriter() {
    var props = baseProps();
    props.put(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:eu-central-1:123456789012:event-bus/my-bus");

    var writer = EventBridgeSinkTask.createWriter(props, new PublishCounters());
    try {
      assertThat(writer).isInstanceOf(EventBridgeWriter.class);
    } finally {
      writer.shutdown();
    }
  }

  @Test
  void v2ConfigurationSelectsCustomEventBusWriter() {
    var props = baseProps();
    props.put(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:eu-central-1:123456789012:event-busv2/my-bus/ABCDEFGHIJKLM1234567890");
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");

    var writer = EventBridgeSinkTask.createWriter(props, new PublishCounters());
    try {
      assertThat(writer).isInstanceOf(EventBridgeV2Writer.class);
    } finally {
      writer.shutdown();
    }
  }

  @Test
  void invalidCombinationFailsAtStartupWithValidatorMessage() {
    var props = baseProps();
    // v2 mode against a classic bus ARN
    props.put(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:eu-central-1:123456789012:event-bus/my-bus");
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> EventBridgeSinkTask.createWriter(props, new PublishCounters()))
        .isInstanceOf(org.apache.kafka.common.config.ConfigException.class)
        .hasMessageContaining("event-busv2");
  }
}
