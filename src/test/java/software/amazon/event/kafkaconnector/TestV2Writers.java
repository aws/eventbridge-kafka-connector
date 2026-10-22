/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

/** Test helper exposing production v2 writer construction to tests outside this package. */
public final class TestV2Writers {

  private TestV2Writers() {}

  public static EventBridgeSinkWriter create(EventBridgeSinkConfig config) {
    return new EventBridgeV2Writer(EventBridgeV2ClientFactory.create(config), config);
  }
}
