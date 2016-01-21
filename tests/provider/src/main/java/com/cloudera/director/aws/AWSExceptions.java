//  (c) Copyright 2015 Cloudera, Inc.

package com.cloudera.director.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.cloudera.director.spi.v1.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v1.model.exception.TransientProviderException;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;

/**
 * Provides utilities for dealing with AWS exceptions.
 * <p/>
 * {@link <a href="http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html" />}
 */
public class AWSExceptions {

  /**
   * Represents an authorization failure.
   */
  private static final String AUTH_FAILURE = "AuthFailure";

  /**
   * Returns an appropriate SPI exception in response to the specified AWS exception.
   *
   * @param e the AWS exception
   * @return the corresponding SPI exception
   */
  public static RuntimeException propagate(AmazonClientException e) {
    if (e instanceof AmazonServiceException) {
      AmazonServiceException ase = (AmazonServiceException) e;
      String errorCode = ase.getErrorCode();
      if (AUTH_FAILURE.equals(errorCode)) {
        return new InvalidCredentialsException(e.getMessage());
      }
    }
    if (e.isRetryable()) {
      return new TransientProviderException(e.getMessage());
    } else {
      return new UnrecoverableProviderException(e.getMessage());
    }
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private AWSExceptions() {
  }
}
