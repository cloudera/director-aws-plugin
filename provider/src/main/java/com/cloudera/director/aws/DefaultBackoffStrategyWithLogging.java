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

package com.cloudera.director.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This behaves the same as the default AWS backoff strategy but with additional
 * logging when backoff happens.
 */
public class DefaultBackoffStrategyWithLogging implements RetryPolicy.BackoffStrategy {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultBackoffStrategyWithLogging.class);

  private static final String BACKOFF_MESSAGE = "AWS will backoff and retry due " +
      "to exception : errorCode=%s, serviceRequest=%s, retriesAttempted=%s, " +
      "delayBeforeNextRetry(ms)=%s";

  @Override
  public long delayBeforeNextRetry(AmazonWebServiceRequest originalRequest,
                                   AmazonClientException exception,
                                   int retriesAttempted) {
    long delayBeforeNextRetry = PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY
        .delayBeforeNextRetry(originalRequest, exception, retriesAttempted);
    logBackoffInformation(originalRequest, exception, retriesAttempted, delayBeforeNextRetry);
    return delayBeforeNextRetry;
  }

  private void logBackoffInformation(AmazonWebServiceRequest originalRequest,
                                     AmazonClientException exception,
                                     int retriesAttempted, long delayBeforeNextRetry) {
    String serviceRequest = originalRequest.getClass().getSimpleName();
    String errorCode = "Unknown";
    if (exception instanceof AmazonServiceException) {
      AmazonServiceException amazonServiceException = (AmazonServiceException) exception;
      errorCode = amazonServiceException.getErrorCode();
    }
    String message = String.format(BACKOFF_MESSAGE, errorCode, serviceRequest,
        retriesAttempted, delayBeforeNextRetry);
    LOG.info(message);
  }
}
