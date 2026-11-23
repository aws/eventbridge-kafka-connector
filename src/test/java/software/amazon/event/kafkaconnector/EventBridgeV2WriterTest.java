/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.eventbridgev2.EventBridgeV2AsyncClient;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequest;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsResponse;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsResultEntry;
import software.amazon.awssdk.services.eventbridgev2.model.SuccessCode;
import software.amazon.event.kafkaconnector.util.PublishCounters;

@ExtendWith(MockitoExtension.class)
public class EventBridgeV2WriterTest {

  private static final String BUS_ARN =
      "arn:aws:events:eu-central-1:123456789012:event-busv2/my-bus/ABCDEFGHIJKLM1234567890";

  @Mock private EventBridgeV2AsyncClient client;

  private EventBridgeSinkConfig configWith(Map<String, Object> extra) {
    var props = new HashMap<String, Object>();
    props.put("aws.eventbridge.connector.id", "someId");
    props.put("aws.eventbridge.region", "eu-central-1");
    props.put("aws.eventbridge.eventbus.arn", BUS_ARN);
    props.put("aws.eventbridge.api.version", "v2");
    props.put("aws.eventbridge.v2.payload.strategy", "raw");
    props.putAll(extra);
    return new EventBridgeSinkConfig(props);
  }

  private static SinkRecord record(long offset) {
    return new SinkRecord("orders", 0, null, null, null, new byte[] {1}, offset);
  }

  private static PutRawEventsResultEntry published(String eventId) {
    return PutRawEventsResultEntry.builder()
        .eventId(eventId)
        .successCode(SuccessCode.PUBLISHED)
        .build();
  }

  @Test
  void publishedEntryIsReportedAsSuccess() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-1"))
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results).hasSize(1);
    assertThat(results.get(0).isSuccess()).isTrue();
    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(client).putRawEvents(captor.capture());
    assertThat(captor.getValue().eventBusArn()).isEqualTo(BUS_ARN);
    assertThat(captor.getValue().entries()).hasSize(1);
  }

  @Test
  void deduplicatedEntryIsSuccessWithDeduplicatedOutcome() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(
                        PutRawEventsResultEntry.builder()
                            .eventId("evt-1")
                            .successCode(SuccessCode.DEDUPLICATED)
                            .build())
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results.get(0).isSuccess()).isTrue();
    assertThat(results.get(0).outcome()).isEqualTo(EventBridgeResult.Outcome.DEDUPLICATED);
  }

  @Test
  void publishedEntryHasPublishedOutcome() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-1"))
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results.get(0).outcome()).isEqualTo(EventBridgeResult.Outcome.PUBLISHED);
  }

  @Test
  void keyFallbacksAreCountedInTheSuppliedCounters() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-1"))
                    .build()));
    var counters = new PublishCounters();
    var config =
        configWith(
            Map.of(
                "aws.eventbridge.v2.event.group.id",
                "record-key",
                "aws.eventbridge.v2.event.group.id.record.key.ack",
                true));
    var writer = new EventBridgeV2Writer(client, config, counters);

    writer.putItems(List.of(record(1)));

    assertThat(counters.keyFallbacks()).isEqualTo(1);
  }

  @Test
  void systemMetadataDoesNotCountTowardTheRequestLimit() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-2"))
                    .build()));
    var probe = new SinkRecord("orders", 0, null, null, null, new byte[0], 2L);
    var metadataBytes =
        software.amazon.event.kafkaconnector.mapping.V2EntryAssembly.provenance(probe)
            .entrySet()
            .stream()
            .mapToLong(
                e ->
                    e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length)
            .sum();
    var dataBytes = (int) (1024 * 1024 - metadataBytes);
    var exact = new SinkRecord("orders", 0, null, null, null, new byte[dataBytes], 2L);
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(exact));

    assertThat(results.get(0).isSuccess()).isTrue();
    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(client).putRawEvents(captor.capture());
    var sm = captor.getValue().entries().get(0).systemMetadata();
    assertThat(sm.eventGroupId()).isNotNull();
    assertThat(sm.deduplicationId()).isNotNull();
  }

  @Test
  void entryLargerThanRequestLimitIsReportedAndNeverSent() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-1"), published("evt-3"))
                    .build()));
    var oversize = new SinkRecord("orders", 0, null, null, null, new byte[1024 * 1024 + 1], 2L);
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1), oversize, record(3)));

    var failed = results.stream().filter(EventBridgeResult::isFailure).toList();
    assertThat(failed).hasSize(1);
    assertThat(failed.get(0).failure().getSinkRecord()).isSameAs(oversize);
    assertThat(failed.get(0).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.REPORT_ONLY);
    assertThat(failed.get(0).failure().getValue().getMessage()).contains("1 MiB");
    assertThat(results).filteredOn(EventBridgeResult::isSuccess).hasSize(2);
    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(client).putRawEvents(captor.capture());
    assertThat(captor.getAllValues())
        .flatExtracting(PutRawEventsRequest::entries)
        .extracting(e -> e.metadata().get("kafka.offset"))
        .containsExactly("1", "3");
  }

  private static PutRawEventsResultEntry failed(String code) {
    return PutRawEventsResultEntry.builder()
        .errorCode(code)
        .errorMessage(code + " message")
        .build();
  }

  private static SinkRecord record(int partition, long offset) {
    return new SinkRecord("orders", partition, null, null, null, new byte[] {1}, offset);
  }

  private static List<SinkRecord> records(int partition, int fromOffset, int count) {
    return java.util.stream.IntStream.range(fromOffset, fromOffset + count)
        .mapToObj(o -> record(partition, o))
        .toList();
  }

  /** Answers each request entry by its Kafka partition and offset. */
  private void respond(
      java.util.function.BiFunction<Integer, Long, PutRawEventsResultEntry> byRecord) {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenAnswer(
            invocation -> {
              PutRawEventsRequest request = invocation.getArgument(0);
              return completedFuture(
                  PutRawEventsResponse.builder()
                      .entries(
                          request.entries().stream()
                              .map(
                                  e ->
                                      byRecord.apply(
                                          Integer.parseInt(e.metadata().get("kafka.partition")),
                                          Long.parseLong(e.metadata().get("kafka.offset"))))
                              .toList())
                      .build());
            });
  }

  private static List<String> sent(List<PutRawEventsRequest> requests) {
    return requests.stream()
        .flatMap(r -> r.entries().stream())
        .map(e -> e.metadata().get("kafka.partition") + ":" + e.metadata().get("kafka.offset"))
        .toList();
  }

  private static List<String> retried(List<? extends EventBridgeResult<?>> results) {
    return results.stream()
        .filter(EventBridgeResult::isFailure)
        .filter(r -> r.failure().getValue().getType() == EventBridgeResult.ErrorType.RETRY)
        .map(r -> r.failure().getSinkRecord())
        .map(r -> r.kafkaPartition() + ":" + r.kafkaOffset())
        .toList();
  }

  private static List<String> range(int partition, int from, int toExclusive) {
    return java.util.stream.IntStream.range(from, toExclusive)
        .mapToObj(o -> partition + ":" + o)
        .toList();
  }

  private List<PutRawEventsRequest> capturedRequests() {
    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(client, org.mockito.Mockito.atLeastOnce()).putRawEvents(captor.capture());
    return captor.getAllValues();
  }

  @Test
  void laterRecordsOfAThrottledGroupAreHeldBackInInputOrder() {
    respond((p, o) -> o == 50 ? failed("ThrottlingException") : published("evt"));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(records(0, 0, 150));

    assertThat(sent(capturedRequests())).containsExactlyElementsOf(range(0, 0, 100));
    var expected = new java.util.ArrayList<>(List.of("0:50"));
    expected.addAll(range(0, 100, 150));
    assertThat(retried(results)).containsExactlyElementsOf(expected);
  }

  @Test
  void otherGroupsInALaterRequestAreStillSent() {
    respond((p, o) -> p == 0 && o == 50 ? failed("ThrottlingException") : published("evt"));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));
    var input = new java.util.ArrayList<>(records(0, 0, 100));
    for (int i = 0; i < 25; i++) {
      input.add(record(1, i));
      input.add(record(0, 100 + i));
    }

    var results = writer.putItems(input);

    var requests = capturedRequests();
    assertThat(requests).hasSize(2);
    assertThat(sent(List.of(requests.get(1)))).containsExactlyElementsOf(range(1, 0, 25));
    var expected = new java.util.ArrayList<>(List.of("0:50"));
    expected.addAll(range(0, 100, 125));
    assertThat(retried(results)).containsExactlyElementsOf(expected);
  }

  @Test
  void requestLevelThrottlingHoldsBackItsGroupsInLaterRequests() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            java.util.concurrent.CompletableFuture.failedFuture(
                software.amazon.awssdk.services.eventbridgev2.model.ThrottlingException.builder()
                    .message("slow down")
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(records(0, 0, 150));

    verify(client, times(1)).putRawEvents(any(PutRawEventsRequest.class));
    assertThat(retried(results)).containsExactlyElementsOf(range(0, 0, 150));
  }

  @Test
  void reportOnlyFailureDoesNotBlockItsGroup() {
    respond((p, o) -> o == 50 ? failed("VALIDATION_ERROR") : published("evt"));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(records(0, 0, 150));

    assertThat(sent(capturedRequests())).containsExactlyElementsOf(range(0, 0, 150));
    assertThat(retried(results)).isEmpty();
  }

  @Test
  void groupingNoneNeverHoldsRecordsBack() {
    respond((p, o) -> o == 50 ? failed("ThrottlingException") : published("evt"));
    var writer =
        new EventBridgeV2Writer(
            client, configWith(Map.of("aws.eventbridge.v2.event.group.id", "none")));

    var results = writer.putItems(records(0, 0, 150));

    assertThat(sent(capturedRequests())).containsExactlyElementsOf(range(0, 0, 150));
    assertThat(retried(results)).containsExactly("0:50");
  }

  @Test
  void laterRecordAcceptedInTheSameRequestStaysPublishedAndIsNotResent() {
    // B (offset 2) is throttled; C (offset 3) of the same group in the same request is accepted
    respond((p, o) -> o == 2 ? failed("ThrottlingException") : published("evt-" + o));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(records(0, 1, 3));

    assertThat(capturedRequests()).hasSize(1);
    assertThat(retried(results)).containsExactly("0:2");
    var c = results.stream().filter(r -> offsetOf(r) == 3).findFirst().orElseThrow();
    assertThat(c.isSuccess()).isTrue();
  }

  private static long offsetOf(EventBridgeResult<?> result) {
    return result.isSuccess()
        ? result.success().getSinkRecord().kafkaOffset()
        : result.failure().getSinkRecord().kafkaOffset();
  }

  @Test
  void perEntryThrottlingIsRetriedAndSiblingsStaySuccessful() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(1)
                    .entries(published("evt-1"), failed("ThrottlingException"), published("evt-3"))
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1), record(2), record(3)));

    assertThat(results).filteredOn(EventBridgeResult::isSuccess).hasSize(2);
    var failure = results.stream().filter(EventBridgeResult::isFailure).findFirst().orElseThrow();
    assertThat(failure.failure().getSinkRecord().kafkaOffset()).isEqualTo(2);
    assertThat(failure.failure().getValue().getType()).isEqualTo(EventBridgeResult.ErrorType.RETRY);
  }

  @Test
  void perEntryLoadSheddingThrottlingIsRetried() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(1)
                    .entries(failed("Throttling"))
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results.get(0).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.RETRY);
  }

  @Test
  void perEntryInternalErrorIsRetried() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(1)
                    .entries(failed("InternalError"))
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results.get(0).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.RETRY);
  }

  @Test
  void unrecognizedOrDifferentlyCasedCodeIsReportOnlyWithCodeAndMessage() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(2)
                    .entries(failed("SomethingNew"), failed("throttlingexception"))
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1), record(2)));

    assertThat(results)
        .allSatisfy(
            r ->
                assertThat(r.failure().getValue().getType())
                    .isEqualTo(EventBridgeResult.ErrorType.REPORT_ONLY));
    assertThat(results.get(0).failure().getValue().getMessage())
        .contains("SomethingNew")
        .contains("SomethingNew message");
  }

  @Test
  void perEntryErrorIsReportOnlyAndDoesNotAffectSiblingsIncludingDeduplicated() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(1)
                    .entries(
                        published("evt-1"),
                        PutRawEventsResultEntry.builder()
                            .errorCode("VALIDATION_ERROR")
                            .errorMessage("payload does not match schema")
                            .build(),
                        PutRawEventsResultEntry.builder()
                            .eventId("evt-3")
                            .successCode(SuccessCode.DEDUPLICATED)
                            .build())
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1), record(2), record(3)));

    assertThat(results).hasSize(3);
    assertThat(results.get(0).isSuccess()).isTrue();
    assertThat(results.get(1).isFailure()).isTrue();
    assertThat(results.get(1).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.REPORT_ONLY);
    assertThat(results.get(1).failure().getValue().getMessage())
        .contains("VALIDATION_ERROR")
        .contains("payload does not match schema");
    assertThat(results.get(1).failure().getSinkRecord().kafkaOffset()).isEqualTo(2);
    assertThat(results.get(2).isSuccess()).isTrue();
  }

  @Test
  void mappingFailuresAreReportedWhileValidRecordsArePublished() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-1"))
                    .build()));
    var tombstone = new SinkRecord("orders", 0, null, null, null, null, 2L);
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1), tombstone));

    assertThat(results).hasSize(2);
    assertThat(results).filteredOn(EventBridgeResult::isSuccess).hasSize(1);
    var failed = results.stream().filter(EventBridgeResult::isFailure).findFirst().orElseThrow();
    assertThat(failed.failure().getSinkRecord()).isSameAs(tombstone);
    assertThat(failed.failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.REPORT_ONLY);
    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(client).putRawEvents(captor.capture());
    assertThat(captor.getValue().entries()).hasSize(1);
  }

  private static java.util.concurrent.CompletableFuture<PutRawEventsResponse> failedWith(
      Throwable t) {
    var future = new java.util.concurrent.CompletableFuture<PutRawEventsResponse>();
    future.completeExceptionally(t);
    return future;
  }

  @Test
  void throttlingFailsAllRecordsInRequestWithRetry() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            failedWith(
                software.amazon.awssdk.services.eventbridgev2.model.ThrottlingException.builder()
                    .message("slow down")
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1), record(2)));

    assertThat(results).hasSize(2).allMatch(EventBridgeResult::isFailure);
    assertThat(results)
        .extracting(r -> r.failure().getValue().getType())
        .containsOnly(EventBridgeResult.ErrorType.RETRY);
  }

  @Test
  void schemaRegistryUnavailableFailsAllRecordsInRequestWithRetry() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            failedWith(
                software.amazon.awssdk.services.eventbridgev2.model
                    .SchemaRegistryUnavailableException.builder()
                    .message("registry unreachable")
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results).hasSize(1);
    assertThat(results.get(0).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.RETRY);
  }

  @Test
  void internalExceptionFailsRecordsWithRetry() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            failedWith(
                software.amazon.awssdk.services.eventbridgev2.model.InternalException.builder()
                    .message("internal")
                    .build()));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results.get(0).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.RETRY);
  }

  @Test
  void clientTimeoutFailsRecordsWithRetry() {
    // a future that never completes forces the writer's SDK call timeout
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(new java.util.concurrent.CompletableFuture<>());
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results.get(0).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.RETRY);
  }

  static java.util.stream.Stream<Throwable> configurationErrors() {
    return java.util.stream.Stream.of(
        software.amazon.awssdk.services.eventbridgev2.model.InvalidInputException.builder()
            .message("bad input")
            .build(),
        software.amazon.awssdk.services.eventbridgev2.model.AccessDeniedException.builder()
            .message("denied")
            .build(),
        software.amazon.awssdk.services.eventbridgev2.model.ResourceNotFoundException.builder()
            .message("no such bus")
            .build());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("configurationErrors")
  void configurationErrorsFailRecordsWithPanic(Throwable error) {
    when(client.putRawEvents(any(PutRawEventsRequest.class))).thenReturn(failedWith(error));
    var writer = new EventBridgeV2Writer(client, configWith(Map.of()));

    var results = writer.putItems(List.of(record(1)));

    assertThat(results.get(0).failure().getValue().getType())
        .isEqualTo(EventBridgeResult.ErrorType.PANIC);
  }

  @Test
  void avroRequestNamesConfiguredSchemaRegistry() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-1"))
                    .build()));
    var glueArn = "arn:aws:glue:eu-central-1:123456789012:registry/orders";
    var writer =
        new EventBridgeV2Writer(
            client,
            configWith(
                Map.of(
                    "aws.eventbridge.v2.content.type",
                    "application/avro",
                    "aws.eventbridge.v2.schema.registry.uri",
                    glueArn)));

    writer.putItems(List.of(record(1)));

    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(client).putRawEvents(captor.capture());
    assertThat(captor.getValue().schemaRegistryConfiguration()).isNotNull();
    assertThat(captor.getValue().schemaRegistryConfiguration().registryUri()).isEqualTo(glueArn);
  }

  @Test
  void cloudEventsStrategyPublishesCloudEventEnvelopes() {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(published("evt-1"))
                    .build()));
    var writer =
        new EventBridgeV2Writer(
            client, configWith(Map.of("aws.eventbridge.v2.payload.strategy", "cloudevents")));
    var jsonRecord = new SinkRecord("orders", 0, null, null, null, "{\"a\":1}", 1L);

    writer.putItems(List.of(jsonRecord));

    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(client).putRawEvents(captor.capture());
    var entry = captor.getValue().entries().get(0);
    var event = new io.cloudevents.jackson.JsonFormat().deserialize(entry.data().asByteArray());
    assertThat(event.getId()).isEqualTo("orders-0-1");
    assertThat(entry.systemMetadata().contentType()).isEqualTo("application/json");
  }
}
