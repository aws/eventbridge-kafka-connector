/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Minimal seam between {@link EventBridgeSinkTask} and a publish implementation, allowing the task
 * to remain agnostic of the EventBridge publish API in use (classic PutEvents or Custom Event Bus
 * PutRawEvents).
 */
public interface EventBridgeSinkWriter {

  /**
   * Ingests the given records into EventBridge.
   *
   * @param records the {@link SinkRecord}s to send
   * @return per-record results with success or failure status information
   */
  List<? extends EventBridgeResult<?>> putItems(List<SinkRecord> records);

  /** Closes underlying clients and releases resources. */
  void shutdown();
}
