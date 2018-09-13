// (c) Copyright 2018 Cloudera, Inc.
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

package com.cloudera.director.aws.common;

import com.google.common.base.Throwables;

import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * Provides utilities for manipulating callables.
 */
public final class Callables2 {

  /**
   * Attempts to run all the specified callables sequentially. Immediately propagates Error
   * and InterruptedException, and accumulates other Exceptions until all callables are run.
   * If at least one exception occurs, this method will eventually throw the first such exception
   * if it is a RuntimeException, or a RuntimeException wrapper otherwise. If multiple exceptions
   * are thrown, subsequent exceptions will be added as suppressed exceptions to the thrown
   * exception, to prevent masking of the original exception.
   *
   * @param callables the callables to run
   * @throws InterruptedException if the process is interrupted
   */
  @SafeVarargs
  public static void callAll(Callable<Void>... callables) throws InterruptedException {
    callAll(RuntimeException.class, Arrays.asList(callables));
  }

  /**
   * Attempts to run all the specified callables sequentially. Immediately propagates Error
   * and InterruptedException, and accumulates other Exceptions until all callables are run.
   * If at least one exception occurs, this method will eventually throw the first such exception
   * if it is a RuntimeException or an instance of the specified exception class, or a
   * RuntimeException wrapper otherwise. If multiple exceptions are thrown, subsequent exceptions
   * will be added as suppressed exceptions to the thrown exception, to prevent masking of the
   * original exception.
   *
   * @param exceptionClass the class of exception
   * @param callables the callables to run
   * @param <E> an additional type of throwable that can be propagated
   * @throws InterruptedException if the process is interrupted
   * @throws E if the first exception thrown by one of the callables is of the specified class
   */
  public static <E extends Throwable> void callAll(Class<E> exceptionClass,
      Iterable<? extends Callable<Void>> callables)
      throws E, InterruptedException {
    Exception ex = null;

    for (Callable callable : callables) {
      try {
        callable.call();
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        if (ex == null) {
          if ((e instanceof RuntimeException) || exceptionClass.isInstance(e)) {
            ex = e;
          } else {
            ex = new RuntimeException(e);
          }
        } else {
          ex.addSuppressed(e);
        }
      }
    }

    if (ex != null) {
      Throwables.propagateIfPossible(ex, exceptionClass);
    }
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private Callables2() {
  }
}
