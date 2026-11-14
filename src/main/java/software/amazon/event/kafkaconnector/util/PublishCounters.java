/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import java.util.concurrent.atomic.AtomicLong;

/** Cumulative record counts by final outcome, shared by a task, its writer, and its reporter. */
public class PublishCounters {

  private final AtomicLong published = new AtomicLong();
  private final AtomicLong deduplicated = new AtomicLong();
  private final AtomicLong dlq = new AtomicLong();
  private final AtomicLong skipped = new AtomicLong();
  private final AtomicLong keyFallbacks = new AtomicLong();

  public void addPublished(long n) {
    published.addAndGet(n);
  }

  public void addDeduplicated(long n) {
    deduplicated.addAndGet(n);
  }

  public void addDlq(long n) {
    dlq.addAndGet(n);
  }

  public void addSkipped(long n) {
    skipped.addAndGet(n);
  }

  public void addKeyFallbacks(long n) {
    keyFallbacks.addAndGet(n);
  }

  public long published() {
    return published.get();
  }

  public long deduplicated() {
    return deduplicated.get();
  }

  public long dlq() {
    return dlq.get();
  }

  public long skipped() {
    return skipped.get();
  }

  public long keyFallbacks() {
    return keyFallbacks.get();
  }
}
