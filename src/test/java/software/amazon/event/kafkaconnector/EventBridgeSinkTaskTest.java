/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.event.kafkaconnector.TestUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.EventBridgeException;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridgev2.EventBridgeV2AsyncClient;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequest;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsResponse;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsResultEntry;
import software.amazon.awssdk.services.eventbridgev2.model.SuccessCode;

@ExtendWith(MockitoExtension.class)
public class EventBridgeSinkTaskTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Mock private EventBridgeAsyncClient eventBridgeAsyncClient;
  @Mock private ErrantRecordReporter dlq;

  private final EventBridgeSinkConfig config =
      new EventBridgeSinkConfig(
          Map.of(
              "aws.eventbridge.retries.max", 10,
              "aws.eventbridge.connector.id", "someId",
              "aws.eventbridge.region", "eu-central-1",
              "aws.eventbridge.eventbus.arn", "something",
              "aws.eventbridge.detail.types", "my-first-${topic}"));

  @BeforeEach
  void resetMocks() {
    reset(eventBridgeAsyncClient, dlq);
  }

  @Test
  @DisplayName("should send records successfully")
  public void sendSuccessfully() {
    var firstResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 10)))
            .build();
    var secondResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(11, 15)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(firstResponse))
        .thenReturn(completedFuture(secondResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(2)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(
            List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
            List.of("11", "12", "13", "14", "15"));
  }

  @Test
  @DisplayName("should resend only retryable failed records")
  public void resendRetryable() {
    var firstResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 10)))
            .build();
    var secondResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(11, 15)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(firstResponse))
        .thenThrow(
            AwsServiceException.builder()
                .cause(EventBridgeException.builder().statusCode(500).build())
                .build())
        .thenReturn(completedFuture(secondResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(3)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(
            List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
            List.of("11", "12", "13", "14", "15"),
            List.of("11", "12", "13", "14", "15"));
  }

  @Test
  @DisplayName("should count each record once when a flush needs a retry round")
  public void countsRetriedRecordsOnce() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 10)))
                    .build()))
        .thenThrow(
            AwsServiceException.builder()
                .cause(EventBridgeException.builder().statusCode(500).build())
                .build())
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(11, 15)))
                    .build()));
    var task = new EventBridgeSinkTask();
    task.startInternal(config, new EventBridgeWriter(eventBridgeAsyncClient, config), dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    assertThat(task.counters().published()).isEqualTo(15);
  }

  /** Writer that fails records whose offset is in {@code reportOnly} and publishes the rest. */
  private static EventBridgeSinkWriter writerFailingReportOnly(List<Long> reportOnly) {
    return new EventBridgeSinkWriter() {
      @Override
      public List<? extends EventBridgeResult<?>> putItems(List<SinkRecord> records) {
        return records.stream()
            .map(
                r ->
                    reportOnly.contains(r.kafkaOffset())
                        ? EventBridgeResult.<String>failure(
                            r, EventBridgeResult.Error.reportOnly("rejected"))
                        : EventBridgeResult.success(r, "evt"))
            .toList();
      }

      @Override
      public void shutdown() {}
    };
  }

  @Test
  @DisplayName("should count report-only failures as dlq, not published")
  public void countsReportOnlyFailuresAsDlq() {
    var task = new EventBridgeSinkTask();
    task.startInternal(config, writerFailingReportOnly(List.of(3L, 7L)), dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 10)));

    assertThat(task.counters().published()).isEqualTo(8);
    assertThat(task.counters().dlq()).isEqualTo(2);
  }

  @Test
  @DisplayName("should count report-only failures as skipped without a dead-letter queue")
  public void countsReportOnlyFailuresAsSkippedWithoutDlq() {
    var task = new EventBridgeSinkTask();
    task.startInternal(config, writerFailingReportOnly(List.of(5L)), null);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 5)));

    assertThat(task.counters().published()).isEqualTo(4);
    assertThat(task.counters().skipped()).isEqualTo(1);
    assertThat(task.counters().dlq()).isZero();
  }

  @Test
  @DisplayName("should count deduplicated results apart from publishes")
  public void countsDeduplicatedApartFromPublished() {
    var writer =
        new EventBridgeSinkWriter() {
          @Override
          public List<? extends EventBridgeResult<?>> putItems(List<SinkRecord> records) {
            return records.stream()
                .map(
                    r ->
                        EventBridgeResult.success(
                            r,
                            "evt",
                            r.kafkaOffset() > 3
                                ? EventBridgeResult.Outcome.DEDUPLICATED
                                : EventBridgeResult.Outcome.PUBLISHED))
                .toList();
          }

          @Override
          public void shutdown() {}
        };
    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 5)));

    assertThat(task.counters().published()).isEqualTo(3);
    assertThat(task.counters().deduplicated()).isEqualTo(2);
  }

  @Test
  @DisplayName("should count only published records in classic mode")
  public void classicModeCountsOnlyPublished() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 5)))
                    .build()));
    var task = new EventBridgeSinkTask();
    task.startInternal(config, new EventBridgeWriter(eventBridgeAsyncClient, config), dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 5)));

    assertThat(task.counters().published()).isEqualTo(5);
    assertThat(task.counters().deduplicated()).isZero();
    assertThat(task.counters().keyFallbacks()).isZero();
  }

  @Test
  @DisplayName("should not retry size exceeding record")
  public void shouldNotRetry() {
    var secondResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(2, 10)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenThrow(
            AwsServiceException.builder()
                .cause(EventBridgeException.builder().statusCode(413).build())
                .build())
        .thenReturn(completedFuture(secondResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    var records = new ArrayList<SinkRecord>();
    records.add(
        new SinkRecord(
            "topic",
            0,
            STRING_SCHEMA,
            "key",
            testSchema,
            new Struct(testSchema).put("id", "#".repeat(1024 * 1024)),
            1));
    records.addAll(OfSinkRecord.withIdsIn(rangeClosed(2, 10)));

    task.put(records);

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(2)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(
            List.of("#".repeat(1024 * 1024)),
            List.of("2", "3", "4", "5", "6", "7", "8", "9", "10"));
  }

  private static EventBridgeSinkConfig v2RecordKeyConfig(String missingKeyPolicy) {
    return new EventBridgeSinkConfig(
        Map.of(
            "aws.eventbridge.retries.max", 2,
            "aws.eventbridge.retries.delay", 0,
            "aws.eventbridge.connector.id", "someId",
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn",
                "arn:aws:events:eu-central-1:123456789012:event-busv2/my-bus/ABCDEFGHIJKLM1234567890",
            "aws.eventbridge.api.version", "v2",
            "aws.eventbridge.v2.payload.strategy", "raw",
            "aws.eventbridge.v2.event.group.id", "record-key",
            "aws.eventbridge.v2.event.group.id.record.key.ack", true,
            "aws.eventbridge.v2.event.group.id.missing.key", missingKeyPolicy));
  }

  private static SinkRecord v2Record(String key, long offset) {
    return new SinkRecord("orders", 0, STRING_SCHEMA, key, null, new byte[] {1}, offset);
  }

  /** Answers every PutRawEvents request with one PUBLISHED entry per request entry. */
  private static void publishEverything(EventBridgeV2AsyncClient client) {
    when(client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenAnswer(
            invocation -> {
              PutRawEventsRequest request = invocation.getArgument(0);
              return completedFuture(
                  PutRawEventsResponse.builder()
                      .failedEntryCount(0)
                      .entries(
                          request.entries().stream()
                              .map(
                                  e ->
                                      PutRawEventsResultEntry.builder()
                                          .eventId("evt")
                                          .successCode(SuccessCode.PUBLISHED)
                                          .build())
                              .toList())
                      .build());
            });
  }

  @Test
  @DisplayName("v2: report policy sends a keyless record to the dead-letter queue")
  public void v2ReportPolicySendsKeylessRecordToDlq() {
    var v2Client = org.mockito.Mockito.mock(EventBridgeV2AsyncClient.class);
    publishEverything(v2Client);
    var v2Config = v2RecordKeyConfig("report");
    var task = new EventBridgeSinkTask();
    task.startInternal(v2Config, new EventBridgeV2Writer(v2Client, v2Config), dlq);
    var keyless = v2Record(null, 2);

    task.put(List.of(v2Record("order-1", 1), keyless, v2Record("order-3", 3)));

    verify(dlq).report(org.mockito.ArgumentMatchers.eq(keyless), any(Throwable.class));
    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(v2Client).putRawEvents(captor.capture());
    assertThat(captor.getValue().entries()).hasSize(2);
    assertThat(task.counters().published()).isEqualTo(2);
    assertThat(task.counters().dlq()).isEqualTo(1);
  }

  @Test
  @DisplayName("v2: fail policy fails the task on a keyless record")
  public void v2FailPolicyFailsTaskOnKeylessRecord() {
    var v2Client = org.mockito.Mockito.mock(EventBridgeV2AsyncClient.class);
    publishEverything(v2Client);
    var v2Config = v2RecordKeyConfig("fail");
    var task = new EventBridgeSinkTask();
    task.startInternal(v2Config, new EventBridgeV2Writer(v2Client, v2Config), dlq);
    var records = List.of(v2Record("order-1", 1), v2Record("", 2));

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> task.put(records))
        .isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class)
        .hasMessageContaining("aws.eventbridge.v2.event.group.id.missing.key");
    org.mockito.Mockito.verifyNoInteractions(dlq);
    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(v2Client).putRawEvents(captor.capture());
    assertThat(captor.getValue().entries())
        .extracting(e -> e.metadata().get("kafka.offset"))
        .containsExactly("1");
  }

  private static EventBridgeSinkConfig v2Config() {
    return new EventBridgeSinkConfig(
        Map.of(
            "aws.eventbridge.retries.max", 2,
            "aws.eventbridge.retries.delay", 0,
            "aws.eventbridge.connector.id", "someId",
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn",
                "arn:aws:events:eu-central-1:123456789012:event-busv2/my-bus/ABCDEFGHIJKLM1234567890",
            "aws.eventbridge.api.version", "v2",
            "aws.eventbridge.v2.payload.strategy", "raw"));
  }

  private static PutRawEventsResultEntry v2Published() {
    return PutRawEventsResultEntry.builder()
        .eventId("evt")
        .successCode(SuccessCode.PUBLISHED)
        .build();
  }

  private static PutRawEventsResultEntry v2Throttled() {
    return PutRawEventsResultEntry.builder()
        .errorCode("ThrottlingException")
        .errorMessage("event group over quota")
        .build();
  }

  private static List<String> offsetsOf(PutRawEventsRequest request) {
    return request.entries().stream().map(e -> e.metadata().get("kafka.offset")).toList();
  }

  @Test
  @DisplayName("v2: a per-entry throttled record is retried and delivered, not dead-lettered")
  public void v2PerEntryThrottledRecordIsRetriedAndDelivered() {
    var v2Client = org.mockito.Mockito.mock(EventBridgeV2AsyncClient.class);
    when(v2Client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder()
                    .failedEntryCount(1)
                    .entries(v2Published(), v2Throttled())
                    .build()))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder().failedEntryCount(0).entries(v2Published()).build()));
    var config = v2Config();
    var task = new EventBridgeSinkTask();
    task.startInternal(config, new EventBridgeV2Writer(v2Client, config), dlq);

    task.put(List.of(v2Record("a", 1), v2Record("b", 2)));

    var captor = ArgumentCaptor.forClass(PutRawEventsRequest.class);
    verify(v2Client, times(2)).putRawEvents(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(EventBridgeSinkTaskTest::offsetsOf)
        .containsExactly(List.of("1", "2"), List.of("2"));
    org.mockito.Mockito.verifyNoInteractions(dlq);
    assertThat(task.counters().published()).isEqualTo(2);
  }

  @Test
  @DisplayName("v2: a record throttled until retries run out fails the task")
  public void v2PerEntryThrottlingUntilRetriesExhaustedFailsTask() {
    var v2Client = org.mockito.Mockito.mock(EventBridgeV2AsyncClient.class);
    when(v2Client.putRawEvents(any(PutRawEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutRawEventsResponse.builder().failedEntryCount(1).entries(v2Throttled()).build()));
    var config = v2Config();
    var task = new EventBridgeSinkTask();
    task.startInternal(config, new EventBridgeV2Writer(v2Client, config), dlq);

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> task.put(List.of(v2Record("a", 1))))
        .isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class);
    verify(v2Client, times(config.maxRetries + 1)).putRawEvents(any(PutRawEventsRequest.class));
    org.mockito.Mockito.verifyNoInteractions(dlq);
    assertThat(task.counters().published()).isZero();
  }

  private <T> Function<PutEventsRequest, List<T>> fromPutEventsRequestDetail(
      Function<JsonNode, T> f) {
    return (PutEventsRequest request) ->
        request.entries().stream()
            .map(PutEventsRequestEntry::detail)
            .map(
                detail -> {
                  try {
                    return f.apply(mapper.readTree(detail));
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .toList();
  }
}
