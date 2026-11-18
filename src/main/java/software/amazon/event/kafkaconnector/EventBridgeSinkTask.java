/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.stream.Stream.empty;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.exceptions.EventBridgePartialFailureResponse;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;
import software.amazon.event.kafkaconnector.util.PropertiesUtil;
import software.amazon.event.kafkaconnector.util.PublishCounters;
import software.amazon.event.kafkaconnector.util.StatusReporter;

public class EventBridgeSinkTask extends SinkTask {

  private final Logger log = ContextAwareLoggerFactory.getLogger(EventBridgeSinkTask.class);
  private EventBridgeSinkWriter eventBridgeWriter;
  private ErrantRecordReporter dlq;
  private EventBridgeSinkConfig config;
  private final PublishCounters counters = new PublishCounters();
  private final StatusReporter statusReporter = new StatusReporter(1, TimeUnit.MINUTES, counters);

  @Override
  public void start(Map<String, String> properties) {
    log.info("Starting Kafka Connect task for EventBridgeSinkConnector");

    var config = new EventBridgeSinkConfig(properties);
    var eventBridgeWriter = createWriter(properties, counters);

    ErrantRecordReporter dlq;
    try {
      dlq = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      dlq = null;
    }
    if (dlq != null) {
      log.info("Dead-letter queue enabled");
    }

    startInternal(config, eventBridgeWriter, dlq);
  }

  /**
   * Creates the publish writer for the configured EventBridge API version. Cross-field validation
   * runs here as well so inconsistent configurations fail at task startup even when the connector
   * validate endpoint was bypassed.
   */
  static EventBridgeSinkWriter createWriter(
      Map<String, String> properties, PublishCounters counters) {
    EventBridgeSinkConfigValidator.validateCrossField(properties);
    var config = new EventBridgeSinkConfig(properties);
    if ("v2".equals(config.apiVersion)) {
      return new EventBridgeV2Writer(EventBridgeV2ClientFactory.create(config), config, counters);
    }
    return new EventBridgeWriter(config);
  }

  void startInternal(
      EventBridgeSinkConfig config,
      EventBridgeSinkWriter eventBridgeWriter,
      ErrantRecordReporter dlq) {
    this.config = config;
    this.eventBridgeWriter = eventBridgeWriter;
    this.dlq = dlq;
    statusReporter.startAsync();
  }

  PublishCounters counters() {
    return counters;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.trace("EventBridgeSinkTask put called with {} records: {}", records.size(), records);
    if (records.isEmpty()) {
      log.trace("Returning early: 0 records received");
      return;
    }

    var remainingRecords = List.copyOf(records);

    var attempts = new AtomicInteger(0);
    var start = OffsetDateTime.now(ZoneOffset.UTC);

    while (!remainingRecords.isEmpty()) {
      attempts.incrementAndGet();
      log.trace(
          "putItems call started: start={} attempts={} maxRetries={}",
          start,
          attempts,
          config.maxRetries);

      var results = eventBridgeWriter.putItems(remainingRecords);
      countSuccesses(results);
      remainingRecords =
          results.stream()
              .filter(EventBridgeResult::isFailure)
              .flatMap(it -> handleFailedEntries(it.failure(), attempts.get()))
              .toList();

      try {
        // TODO: improve retry delay implementation
        TimeUnit.MILLISECONDS.sleep(config.retriesDelay);
      } catch (InterruptedException ie) {
        throw new ConnectException(ie);
      }
    }

    var completion = OffsetDateTime.now(ZoneOffset.UTC);
    log.trace(
        "putItems call completed: start={} completion={} durationMillis={} "
            + "attempts={} "
            + "maxRetries={}",
        start,
        completion,
        Duration.between(start, completion).toMillis(),
        attempts,
        config.maxRetries);
  }

  // a record succeeds in exactly one round, because only failures go into the next round
  private void countSuccesses(List<? extends EventBridgeResult<?>> results) {
    for (var result : results) {
      if (result.isSuccess()) {
        if (result.outcome() == EventBridgeResult.Outcome.DEDUPLICATED) {
          counters.addDeduplicated(1);
        } else {
          counters.addPublished(1);
        }
      }
    }
  }

  private Stream<SinkRecord> handleFailedEntries(
      MappedSinkRecord<EventBridgeResult.Error> errorRecord, Integer attempts) {

    var error = errorRecord.getValue();
    var message = error.getMessage();
    var cause = error.getCause();
    switch (error.getType()) {
      case REPORT_ONLY:
        var failure =
            new EventBridgePartialFailureResponse(errorRecord.getSinkRecord(), message, cause);
        if (dlq != null) {
          log.trace("Sending record to dead-letter queue: {}", errorRecord);
          dlq.report(errorRecord.getSinkRecord(), failure);
          counters.addDlq(1);
        } else {
          log.warn("Dead-letter queue not configured: skipping failed record", failure);
          counters.addSkipped(1);
        }
        return empty();

      case RETRY:
        if (attempts > config.maxRetries) {
          log.error(
              "Not retrying failed putItems call: reached max retries attempts={} maxRetries={} "
                  + "errorMessage={}",
              attempts,
              config.maxRetries,
              message,
              cause);
          throw new ConnectException(cause);
        }

        log.warn(
            "Retrying failed putItems call: attempts={} maxRetries={} errorMessage={}",
            attempts,
            config.maxRetries,
            message,
            cause);

        return Stream.of(errorRecord.getSinkRecord());

      case PANIC:
        log.error(
            "Non-retryable failed put call: failing connector errorMessage={}", message, cause);
        throw new ConnectException(cause);
    }
    return Stream.empty();
  }

  @Override
  public String version() {
    return PropertiesUtil.getConnectorVersion();
  }

  @Override
  public void stop() {
    log.trace("Stopping sink task");
    if (eventBridgeWriter != null) {
      eventBridgeWriter.shutdown();
    }

    if (statusReporter.isRunning()) {
      statusReporter.stopAsync();
    }
  }
}
