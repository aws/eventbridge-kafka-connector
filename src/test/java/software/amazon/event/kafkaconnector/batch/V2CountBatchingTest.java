/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class V2CountBatchingTest {

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
    var batches = new V2CountBatching().apply(items(250));

    assertThat(batches).extracting(List::size).containsExactly(100, 100, 50);
  }

  @Test
  void preservesPerPartitionOrderWithinAndAcrossBatches() {
    var input = items(250);

    var flattened =
        new V2CountBatching()
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
    assertThat(new V2CountBatching().apply(List.of())).isEmpty();
  }
}
