/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.retry;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Outcome.DEDUPLICATED;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Outcome.PUBLISHED;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.eventbridgev2.EventBridgeV2AsyncClient;
import software.amazon.awssdk.services.eventbridgev2.model.AccessDeniedException;
import software.amazon.awssdk.services.eventbridgev2.model.InvalidInputException;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequest;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsRequestEntry;
import software.amazon.awssdk.services.eventbridgev2.model.PutRawEventsResponse;
import software.amazon.awssdk.services.eventbridgev2.model.ResourceNotFoundException;
import software.amazon.awssdk.services.eventbridgev2.model.SuccessCode;
import software.amazon.event.kafkaconnector.batch.V2Batching;
import software.amazon.event.kafkaconnector.mapping.CloudEventsMapper;
import software.amazon.event.kafkaconnector.mapping.RawPayloadMapper;
import software.amazon.event.kafkaconnector.mapping.SchemaRegistryConfigurations;
import software.amazon.event.kafkaconnector.mapping.V2EntryAssembly;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;
import software.amazon.event.kafkaconnector.util.PublishCounters;

/**
 * Publishes Kafka records to an EventBridge Custom Event Bus via PutRawEvents.
 *
 * <p>Error mapping: per-entry {@code ThrottlingException} and {@code InternalError} are retried;
 * other per-entry error codes are report-only; configuration errors (invalid input, access denied,
 * resource not found) panic; all other request-level failures, including throttling, schema
 * registry unavailability, internal errors, and timeouts, are retried. Entries over the 1 MiB
 * request limit are reported without being sent.
 */
public class EventBridgeV2Writer implements EventBridgeSinkWriter {

  private static final int SDK_TIMEOUT = 5000; // timeout in milliseconds for SDK calls

  private final EventBridgeV2AsyncClient client;
  private final EventBridgeSinkConfig config;
  private final Function<SinkRecord, EventBridgeResult<PutRawEventsRequestEntry>> mapper;
  private final V2Batching batching = new V2Batching();

  public EventBridgeV2Writer(EventBridgeV2AsyncClient client, EventBridgeSinkConfig config) {
    this(client, config, new PublishCounters());
  }

  public EventBridgeV2Writer(
      EventBridgeV2AsyncClient client, EventBridgeSinkConfig config, PublishCounters counters) {
    this.client = client;
    this.config = config;
    var assembly = new V2EntryAssembly(config, counters);
    this.mapper =
        "cloudevents".equals(config.v2PayloadStrategy)
            ? new CloudEventsMapper(config, assembly)::map
            : new RawPayloadMapper(config, assembly)::map;
  }

  @Override
  public List<EventBridgeResult<String>> putItems(List<SinkRecord> records) {
    var mapped = new ArrayList<MappedSinkRecord<PutRawEventsRequestEntry>>();
    var results = new ArrayList<EventBridgeResult<String>>();
    for (var record : records) {
      var result = mapper.apply(record);
      if (result.isFailure()) {
        results.add(failure(result.failure()));
        continue;
      }
      var entry = result.success().getValue();
      if (V2Batching.size(entry) > V2Batching.MAX_BATCH_BYTES) {
        results.add(failure(record, reportOnly(oversizeMessage(entry))));
      } else {
        mapped.add(result.success());
      }
    }

    for (var batch : batching.apply(mapped)) {
      results.addAll(send(batch));
    }
    return results;
  }

  private List<EventBridgeResult<String>> send(
      List<MappedSinkRecord<PutRawEventsRequestEntry>> batch) {
    try {
      var requestBuilder =
          PutRawEventsRequest.builder()
              .eventBusArn(config.eventBusArn)
              .entries(batch.stream().map(MappedSinkRecord::getValue).toList());
      SchemaRegistryConfigurations.from(config)
          .ifPresent(requestBuilder::schemaRegistryConfiguration);
      var response = client.putRawEvents(requestBuilder.build()).get(SDK_TIMEOUT, MILLISECONDS);
      return mapResponse(batch, response);
    } catch (ExecutionException e) {
      var error = isConfigurationError(e.getCause()) ? panic(e.getCause()) : retry(e);
      return failAll(batch, error);
    } catch (TimeoutException e) {
      return failAll(batch, retry(e));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return failAll(batch, retry(e));
    } catch (Exception e) {
      return failAll(batch, panic(e));
    }
  }

  /** Per-entry error codes that are transient and retried under the configured retry policy. */
  static final Set<String> RETRYABLE_ENTRY_ERROR_CODES =
      Set.of("ThrottlingException", "InternalError");

  private static List<EventBridgeResult<String>> mapResponse(
      List<MappedSinkRecord<PutRawEventsRequestEntry>> batch, PutRawEventsResponse response) {
    var results = new ArrayList<EventBridgeResult<String>>();
    for (int i = 0; i < batch.size(); i++) {
      var sinkRecord = batch.get(i).getSinkRecord();
      var resultEntry = response.entries().get(i);
      if (resultEntry.errorCode() != null) {
        var message = String.format("%s: %s", resultEntry.errorCode(), resultEntry.errorMessage());
        var error =
            RETRYABLE_ENTRY_ERROR_CODES.contains(resultEntry.errorCode())
                ? retry(new RetriableException(message))
                : reportOnly(message);
        results.add(failure(sinkRecord, error));
      } else {
        var outcome =
            resultEntry.successCode() == SuccessCode.DEDUPLICATED ? DEDUPLICATED : PUBLISHED;
        results.add(success(sinkRecord, resultEntry.eventId(), outcome));
      }
    }
    return results;
  }

  private static String oversizeMessage(PutRawEventsRequestEntry entry) {
    return String.format(
        "Entry is %d bytes, larger than the 1 MiB (%d bytes) PutRawEvents request limit counting"
            + " data and metadata; not sent",
        V2Batching.size(entry), V2Batching.MAX_BATCH_BYTES);
  }

  private static boolean isConfigurationError(Throwable cause) {
    return cause instanceof InvalidInputException
        || cause instanceof AccessDeniedException
        || cause instanceof ResourceNotFoundException;
  }

  private static List<EventBridgeResult<String>> failAll(
      List<MappedSinkRecord<PutRawEventsRequestEntry>> batch, EventBridgeResult.Error error) {
    return batch.stream()
        .map(it -> EventBridgeResult.<String>failure(it.getSinkRecord(), error))
        .toList();
  }

  @Override
  public void shutdown() {
    client.close();
  }
}
