/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

public class EventBridgeV2ClientFactoryTest {

  private Map<String, Object> v2Props() {
    var props = new HashMap<String, Object>();
    props.put("aws.eventbridge.connector.id", "someId");
    props.put("aws.eventbridge.region", "eu-central-1");
    props.put(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:eu-central-1:123456789012:event-busv2/my-bus/ABCDEFGHIJKLM1234567890");
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    // explicit static provider so tests do not depend on ambient AWS credentials
    props.put(
        "aws.eventbridge.auth.credentials_provider.class",
        StaticCredentialsProvider.class.getName());
    return props;
  }

  public static class StaticCredentialsProvider
      implements software.amazon.awssdk.auth.credentials.AwsCredentialsProvider {
    @Override
    public software.amazon.awssdk.auth.credentials.AwsCredentials resolveCredentials() {
      return software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("akid", "secret");
    }
  }

  @Test
  void createsClientWithConfiguredRegionAndEndpoint() {
    var props = v2Props();
    props.put("aws.eventbridge.endpoint.uri", "http://localhost:4566");
    var config = new EventBridgeSinkConfig(props);

    try (var client = EventBridgeV2ClientFactory.create(config)) {
      var clientConfig = client.serviceClientConfiguration();
      assertThat(clientConfig.region()).isEqualTo(Region.of("eu-central-1"));
      assertThat(clientConfig.endpointOverride()).contains(URI.create("http://localhost:4566"));
    }
  }

  @Test
  void failsFastWhenCredentialsCannotBeResolved() {
    var props = v2Props();
    props.put(
        "aws.eventbridge.auth.credentials_provider.class",
        UnresolvableCredentialsProvider.class.getName());
    var config = new EventBridgeSinkConfig(props);

    org.junit.jupiter.api.Assertions.assertThrows(
        org.apache.kafka.connect.errors.ConnectException.class,
        () -> EventBridgeV2ClientFactory.create(config));
  }

  public static class UnresolvableCredentialsProvider
      implements software.amazon.awssdk.auth.credentials.AwsCredentialsProvider {
    @Override
    public software.amazon.awssdk.auth.credentials.AwsCredentials resolveCredentials() {
      throw new IllegalStateException("no credentials");
    }
  }
}
