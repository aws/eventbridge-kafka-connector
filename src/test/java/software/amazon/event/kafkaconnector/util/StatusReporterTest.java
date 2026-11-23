/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import static ch.qos.logback.classic.Level.INFO;
import static ch.qos.logback.classic.Level.TRACE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static software.amazon.event.kafkaconnector.TestUtils.toFixedGitCommitId;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.TestUtils.ListAppender;

public class StatusReporterTest {

  private static ListAppender appender;

  @BeforeAll
  public static void setup() {
    appender = ListAppender.of(StatusReporter.class, TRACE);
    appender.start();
  }

  @AfterAll
  public static void teardown() {
    appender.detach();
  }

  @AfterEach
  public void clearLoggingEvents() {
    appender.clear();
  }

  @Test
  @DisplayName("Status Reporter should log cumulative counts by outcome")
  public void shouldContainLoggingEvent() {
    var counters = new PublishCounters();
    counters.addPublished(1);
    counters.addDeduplicated(2);
    counters.addDlq(3);
    counters.addSkipped(4);
    counters.addKeyFallbacks(5);
    var sut = new StatusReporter(100, MILLISECONDS, counters);
    sut.startAsync();

    await()
        .atMost(5, SECONDS)
        .untilAsserted(
            () ->
                assertThat(appender.getLoggingEvents())
                    .filteredOn(level(INFO))
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .map(toFixedGitCommitId)
                    .contains("[GitCommitId] Starting status reporter")
                    .contains(
                        "[GitCommitId] Total records: published=1 deduplicated=2 dlq=3 skipped=4"
                            + " keyFallbacks=5"));

    sut.stopAsync();
  }

  private static Predicate<? super ILoggingEvent> level(Level expectedLevel) {
    return (event) -> event.getLevel() == expectedLevel;
  }
}
