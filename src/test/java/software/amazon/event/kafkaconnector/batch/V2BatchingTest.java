/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class V2BatchingTest {

  private static List<MappedSinkRecord<PutRawEventsRequestEntry>> items(int count) {
    return IntStream.range(0, count)
        .mapToObj(
            i ->
                new MappedSinkRecord<>(
                    new SinkRecord("orders", i % 3, null, null, null, new byte[] {1}, i),
                    PutRawEventsRequestEntry.builder().build()))
        .toList();
  }

  @Test
  void splitsIntoRequestsOfAtMostOneHundredEntries() {
    var batches = new V2Batching().apply(items(250));

    assertThat(batches).extracting(List::size).containsExactly(100, 100, 50);
  }

  @Test
  void preservesPerPartitionOrderWithinAndAcrossBatches() {
    var input = items(250);

    var flattened =
        new V2Batching()
            .apply(input).stream()
                .flatMap(List::stream)
                .map(MappedSinkRecord::getSinkRecord)
                .toList();

    for (int partition = 0; partition < 3; partition++) {
      final int p = partition;
      var offsets =
          flattened.stream()
              .filter(r -> r.kafkaPartition() == p)
              .map(SinkRecord::kafkaOffset)
              .toList();
      assertThat(offsets).isSorted();
    }
    assertThat(flattened).hasSize(250);
  }

  @Test
  void emptyInputProducesNoRequests() {
    assertThat(new V2Batching().apply(List.of())).isEmpty();
  }

  @Test
  void entrySizeCountsDataMetadataContentTypeAndIdsAsUtf8Bytes() {
    var entry =
        PutRawEventsRequestEntry.builder()
            .data(SdkBytes.fromByteArray(new byte[10]))
            .metadata(Map.of("kafka.topic", "größe")) // 11 + 7 bytes
            .systemMetadata(
                sm ->
                    sm.contentType("application/json") // 16
                        .eventGroupId("orders-2") // 8
                        .deduplicationId("orders-2-41")) // 11
            .build();

    assertThat(V2Batching.size(entry)).isEqualTo(10 + 11 + 7 + 16 + 8 + 11);
  }

  private static MappedSinkRecord<PutRawEventsRequestEntry> sized(
      int partition, long offset, int dataBytes) {
    return new MappedSinkRecord<>(
        new SinkRecord("orders", partition, null, null, null, new byte[] {1}, offset),
        PutRawEventsRequestEntry.builder()
            .data(SdkBytes.fromByteArray(new byte[dataBytes]))
            .build());
  }

  private static long requestSize(List<MappedSinkRecord<PutRawEventsRequestEntry>> batch) {
    return batch.stream().mapToLong(it -> V2Batching.size(it.getValue())).sum();
  }

  @Test
  void largeEntriesSplitIntoRequestsAtOrUnderOneMebibyte() {
    var input = IntStream.range(0, 100).mapToObj(i -> sized(i % 3, i, 20 * 1024)).toList();

    var batches = new V2Batching().apply(input);

    assertThat(batches).hasSizeGreaterThan(1);
    assertThat(batches).allSatisfy(b -> assertThat(requestSize(b)).isLessThanOrEqualTo(1_048_576));
    assertThat(batches.stream().flatMap(List::stream).toList()).containsExactlyElementsOf(input);
  }

  @Test
  void entryThatExactlyFillsTheLimitIsSentAlone() {
    var exact = (int) V2Batching.MAX_BATCH_BYTES;
    var input = List.of(sized(0, 1, 10), sized(0, 2, exact), sized(0, 3, 10));

    var batches = new V2Batching().apply(input);

    assertThat(batches).extracting(List::size).containsExactly(1, 1, 1);
    assertThat(requestSize(batches.get(1))).isEqualTo(V2Batching.MAX_BATCH_BYTES);
  }

  @Test
  void sizeSplitsPreservePerPartitionOrder() {
    var input = IntStream.range(0, 90).mapToObj(i -> sized(i % 3, i, 50 * 1024)).toList();

    var flattened =
        new V2Batching()
            .apply(input).stream()
                .flatMap(List::stream)
                .map(MappedSinkRecord::getSinkRecord)
                .toList();

    for (int partition = 0; partition < 3; partition++) {
      final int p = partition;
      assertThat(
              flattened.stream()
                  .filter(r -> r.kafkaPartition() == p)
                  .map(SinkRecord::kafkaOffset)
                  .toList())
          .isSorted()
          .hasSize(30);
    }
  }
}
