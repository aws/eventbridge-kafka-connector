/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

/**
 * Count-only batching for PutRawEvents: consecutive chunks of at most {@value #MAX_BATCH_ITEMS}
 * entries, preserving input order. No byte limit is documented for the Custom Event Bus; oversize
 * entries surface as per-entry service errors.
 */
public class V2CountBatching {

  static final int MAX_BATCH_ITEMS = 100;

  public List<List<MappedSinkRecord<PutRawEventsRequestEntry>>> apply(
      List<MappedSinkRecord<PutRawEventsRequestEntry>> items) {
    var batches = new ArrayList<List<MappedSinkRecord<PutRawEventsRequestEntry>>>();
    for (int start = 0; start < items.size(); start += MAX_BATCH_ITEMS) {
      batches.add(items.subList(start, Math.min(start + MAX_BATCH_ITEMS, items.size())));
    }
    return batches;
  }
}
