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

import static com.cloudera.director.aws.ec2.EC2Retryer.getTimeout;
import static com.cloudera.director.aws.ec2.EC2Retryer.retryUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException;
import com.cloudera.director.aws.shaded.org.joda.time.DateTime;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class EC2RetryerTest {

  @Test
  public void testRetrySucceeds() throws Exception {
    AmazonServiceException exception = new AmazonServiceException(EC2Provider.INVALID_INSTANCE_ID_NOT_FOUND);
    exception.setErrorCode(EC2Provider.INVALID_INSTANCE_ID_NOT_FOUND);
    Integer result = 1;

    Callable<Integer> task = mock(Callable.class);
    when(task.call())
        .thenThrow(exception)
        .thenThrow(exception)
        .thenReturn(result);

    assertThat(result).isEqualTo(retryUntil(task));
    verify(task, times(3)).call();
  }

  @Test(timeout = 5000)
  public void testRetryCanceledThrowsInterruptedException() throws Exception {
    final Thread currentThread = Thread.currentThread();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        currentThread.interrupt();
      }
    });
    t.start();

    AmazonServiceException exception = new AmazonServiceException(EC2Provider.INVALID_INSTANCE_ID_NOT_FOUND);
    exception.setErrorCode(EC2Provider.INVALID_INSTANCE_ID_NOT_FOUND);
    Callable<Integer> task = mock(Callable.class);
    when(task.call()).thenThrow(exception);

    try {
      retryUntil(task);
      fail("expected exception didn't thrown");
    } catch (InterruptedException e) {
      verify(task, atLeastOnce()).call();
    }
  }

  @Test
  public void testOtherExceptionWillNotTriggerRetry() throws Exception {
    Callable<Integer> task = mock(Callable.class);
    when(task.call()).thenThrow(new RuntimeException());

    try {
      retryUntil(task);
      fail("expected exception didn't thrown");
    } catch (ExecutionException e) {
      assertThat(RuntimeException.class.isInstance(e.getCause())).isTrue();
      verify(task, times(1)).call();
    }
  }

  @Test
  public void testGetDuration() {
    getTimeout(DateTime.now().plus(10l));
    getTimeout(DateTime.now().minus(10l));
  }
}
