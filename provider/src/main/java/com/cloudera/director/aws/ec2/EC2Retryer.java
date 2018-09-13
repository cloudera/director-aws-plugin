// (c) Copyright 2017 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.director.aws.ec2;

import com.cloudera.director.aws.AWSExceptions;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

public class EC2Retryer {

  // backoff not resulted from contention, so fixed interval
  private static final Duration DEFAULT_BACKOFF = Duration.standardSeconds(5);
  private static final Duration DEFAULT_TIMEOUT = Duration.millis(Long.MAX_VALUE);

  private EC2Retryer() {
    throw new IllegalStateException("static class");
  }

  public static <T> T retryUntil(Callable<T> func)
      throws InterruptedException, RetryException, ExecutionException {
    return retryUntil(func, DEFAULT_TIMEOUT);
  }

  public static <T> T retryUntil(Callable<T> func, DateTime timeout)
      throws InterruptedException, RetryException, ExecutionException {
    return retryUntil(func, timeout, DEFAULT_BACKOFF);
  }

  public static <T> T retryUntil(Callable<T> func, Duration timeout)
      throws InterruptedException, RetryException, ExecutionException {
    return retryUntil(func, timeout, DEFAULT_BACKOFF);
  }

  public static <T> T retryUntil(Callable<T> func, DateTime timeout, Duration backoff)
      throws InterruptedException, RetryException, ExecutionException {
    return retryUntil(func, getTimeout(timeout), backoff);
  }

  public static <T> T retryUntil(Callable<T> func, Duration timeout, Duration backoff)
      throws InterruptedException, RetryException, ExecutionException {

    Retryer<T> retryer = RetryerBuilder.<T>newBuilder()
        .withWaitStrategy(WaitStrategies.fixedWait(backoff.getMillis(), TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(timeout.getMillis(), TimeUnit.MILLISECONDS))
        .retryIfException(AWSExceptions::isNotFound)
        .build();

    try {
      return retryer.call(func);
    } catch (RetryException e) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      throw e;
    }
  }

  @VisibleForTesting
  static Duration getTimeout(DateTime timeout) {
    DateTime now = DateTime.now();

    if (timeout.isBefore(now)) {
      return Duration.ZERO;
    }

    return new Interval(now, timeout).toDuration();
  }
}
