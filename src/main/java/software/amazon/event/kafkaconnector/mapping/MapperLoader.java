/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

/** Loads the configured detail-type and time mapper implementations. */
final class MapperLoader {

  private MapperLoader() {}

  static DetailTypeMapper detailTypeMapper(EventBridgeSinkConfig config) {
    try {
      var mapper =
          (DetailTypeMapper)
              Class.forName(config.detailTypeMapperClass).getDeclaredConstructor().newInstance();
      mapper.configure(config);
      return mapper;
    } catch (Exception e) {
      // This will already be verified in the Config Validator
      throw new RuntimeException("Topic to Detail-Type Mapper Class can't be loaded: ", e);
    }
  }

  static TimeMapper timeMapper(EventBridgeSinkConfig config) {
    try {
      return (TimeMapper)
          Class.forName(config.timeMapperClass).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      // This will already be verified in the Config Validator
      throw new RuntimeException("Time Mapper Class can't be loaded: ", e);
    }
  }
}
