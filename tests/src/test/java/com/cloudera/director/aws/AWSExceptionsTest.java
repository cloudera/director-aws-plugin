// (c) Copyright 2016 Cloudera, Inc.
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

import com.cloudera.director.aws.shaded.com.amazonaws.AmazonClientException;
import com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException;
import com.cloudera.director.spi.v1.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v1.model.exception.TransientProviderException;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import org.junit.Test;

public class AWSExceptionsTest {

  @Test(expected = InvalidCredentialsException.class)
  public void testUnauthorizedOperationException() {
    AmazonServiceException e =
        new AmazonServiceException("Test Amazon UnauthorizedOperation exception");
    e.setErrorCode("UnauthorizedOperation");
    AWSExceptions.propagate(e);
  }

  @Test(expected = InvalidCredentialsException.class)
  public void testAuthFailureException() {
    AmazonServiceException e =
        new AmazonServiceException("Test Amazon AuthFailure exception");
    e.setErrorCode("AuthFailure");
    AWSExceptions.propagate(e);
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testClientError() {
    AmazonServiceException e =
        new AmazonServiceException("Test Amazon client error");
    e.setErrorType(AmazonServiceException.ErrorType.Client);
    AWSExceptions.propagate(e);
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testOperationNotPermittedError() {
    AmazonServiceException e =
        new AmazonServiceException("Test Amazon client error");
    e.setErrorCode("OperationNotPermitted");
    AWSExceptions.propagate(e);
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testUnretryableException() {
    AmazonClientException e = new AmazonClientException("Test un-retryable error") {
      @Override
      public boolean isRetryable() {
        return false;
      }
    };
    AWSExceptions.propagate(e);
  }

  @Test(expected = TransientProviderException.class)
  public void testTransientProviderException() {
    AmazonServiceException e =
        new AmazonServiceException("Test TransientProviderException");
    AWSExceptions.propagate(e);
  }
}
