//  (c) Copyright 2015 Cloudera, Inc.

package com.cloudera.director.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.cloudera.director.spi.v2.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v2.model.exception.TransientProviderException;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Provides utilities for dealing with AWS exceptions.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html">AWS Errors</a>
 */
public class AWSExceptions {

  // The set of error codes representing authorization failures.
  private static final Set<String> AUTHORIZATION_ERROR_CODES = ImmutableSet.of(
      "AuthFailure",
      "UnauthorizedOperation"
  );


  /**
   * Propagates exception as an unrecoverable error when the relevant
   * indicators are present.
   *
   * @param e the Amazon client exception
   */
  public static void propagateIfUnrecoverable(AmazonClientException e) {
    if (e instanceof AmazonServiceException) {
      AmazonServiceException ase = (AmazonServiceException) e;
      if (AUTHORIZATION_ERROR_CODES.contains(ase.getErrorCode())) {
        throw new InvalidCredentialsException(ase.getErrorMessage(), ase);
      }

      // All exceptions that represent client errors are unrecoverable because the request itself is wrong
      // See {@see AmazonServiceException#ErrorType}
      // * OperationNotPermitted exception is unrecoverable. This can happen when terminating an
      //   instance that has termination protection enabled, or trying to detach the primary
      //   network interface (eth0) from an instance.
      // * Unsupported exception is also unrecoverable, since it represents an unsupported request.
      //   See docs at http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
      // * InvalidParameterValue is unrecoverable, as one of the parameters supplied by the user is invalid.
      if (ase.getErrorType() == AmazonServiceException.ErrorType.Client ||
        "OperationNotPermitted".equals(ase.getErrorCode()) ||
        "Unsupported".equals(ase.getErrorCode()) ||
        "InvalidParameterValue".equals(ase.getErrorCode())) {
        throw new UnrecoverableProviderException(ase.getErrorMessage(), ase);
      }
    }

    if (!e.isRetryable()) {
      throw new UnrecoverableProviderException(e.getMessage(), e);
    }
  }

  /**
   * Returns an appropriate SPI exception in response to the specified AWS exception.
   *
   * @param e the AWS exception
   * @return the corresponding SPI exception
   */
  public static RuntimeException propagate(AmazonClientException e) {
    propagateIfUnrecoverable(e);

    // otherwise assume this is a transient error
    throw new TransientProviderException(e.getMessage(), e);
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private AWSExceptions() {
  }
}
