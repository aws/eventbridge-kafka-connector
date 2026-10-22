/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import java.util.Optional;
import software.amazon.awssdk.services.eventbridgev2.model.SchemaRegistryConfiguration;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

/** Builds the request-level schema registry configuration for PutRawEvents. */
public final class SchemaRegistryConfigurations {

  private SchemaRegistryConfigurations() {}

  /**
   * Registry settings for the request, present only for raw-strategy Avro/Protobuf content types.
   * EventBridge ignores registries for other content types, and the config validator rejects them.
   */
  public static Optional<SchemaRegistryConfiguration> from(EventBridgeSinkConfig config) {
    var openFormat =
        "application/avro".equals(config.v2ContentType)
            || "application/protobuf".equals(config.v2ContentType);
    if (!"raw".equals(config.v2PayloadStrategy) || !openFormat) {
      return Optional.empty();
    }
    var builder = SchemaRegistryConfiguration.builder().registryUri(config.v2SchemaRegistryUri);
    if (!config.v2SchemaRegistryConnectionArn.isBlank()) {
      builder.confluentPublicRegistryConfiguration(
          c -> c.connectionArn(config.v2SchemaRegistryConnectionArn));
    }
    return Optional.of(builder.build());
  }
}
