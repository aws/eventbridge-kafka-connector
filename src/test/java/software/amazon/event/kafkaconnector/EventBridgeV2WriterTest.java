/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
