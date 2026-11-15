/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

/**
 * Batching for PutRawEvents: consecutive requests of at most {@value #MAX_BATCH_ITEMS} entries
 * whose combined {@link #size} is at most {@link #MAX_BATCH_BYTES}, preserving input order. An
 * entry larger than the byte limit must be rejected before batching; if one gets here, it is placed
 * in a request of its own.
 */
public class V2Batching {

  static final int MAX_BATCH_ITEMS = 100;

  /** A request's entries may total at most 1 MiB, counting data and metadata. */
  public static final long MAX_BATCH_BYTES = 1024 * 1024;

  /**
   * Request size of an entry as the service counts it: the bytes of the data plus the UTF-8 bytes
   * of each metadata key and value. System metadata (content type, event group and deduplication
   * ids) does not count.
   */
  public static long size(PutRawEventsRequestEntry entry) {
    long size = entry.data() == null ? 0 : entry.data().asByteArray().length;
    if (entry.metadata() != null) {
      for (var e : entry.metadata().entrySet()) {
        size += utf8(e.getKey()) + utf8(e.getValue());
      }
    }
    return size;
  }

  private static int utf8(String value) {
    return value == null ? 0 : value.getBytes(UTF_8).length;
  }

  public List<List<MappedSinkRecord<PutRawEventsRequestEntry>>> apply(
      List<MappedSinkRecord<PutRawEventsRequestEntry>> items) {
    var batches = new ArrayList<List<MappedSinkRecord<PutRawEventsRequestEntry>>>();
    var current = new ArrayList<MappedSinkRecord<PutRawEventsRequestEntry>>();
    long currentSize = 0;
    for (var item : items) {
      var itemSize = size(item.getValue());
      if (!current.isEmpty()
          && (current.size() >= MAX_BATCH_ITEMS || currentSize + itemSize > MAX_BATCH_BYTES)) {
        batches.add(current);
        current = new ArrayList<>();
        currentSize = 0;
      }
      current.add(item);
      currentSize += itemSize;
    }
    if (!current.isEmpty()) {
      batches.add(current);
    }
    return batches;
  }
}
