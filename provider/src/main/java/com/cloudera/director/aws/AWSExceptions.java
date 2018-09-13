// (c) Copyright 2015 Cloudera, Inc.
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
import com.amazonaws.services.ec2.model.StateReason;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.spi.v2.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v2.model.exception.TransientProviderException;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utilities for dealing with AWS exceptions.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html">AWS Errors</a>
 */
public class AWSExceptions {

  private static final Logger LOG = LoggerFactory.getLogger(AWSExceptions.class);

  /**
   * Error code suffix indicating that a resource was not found.
   */
  private static final String NOT_FOUND_ERROR_CODE = ".NotFound";

  private static final String RESOURCE_NOT_FOUND = "Resource not found, might be a transient error";

  /**
   * Returns whether a {@code Throwable} indicates that an AWS resource cannot be found.
   *
   * @param throwable a throwable
   * @return whether a {@code Throwable} indicates that an AWS resource cannot be found
   */
  public static boolean isNotFound(Throwable throwable) {
    boolean retryNeeded = AmazonServiceException.class.isInstance(throwable)
        && ((AmazonServiceException) throwable).getErrorCode().endsWith(NOT_FOUND_ERROR_CODE);

    if (retryNeeded) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(RESOURCE_NOT_FOUND, throwable);
      } else if (LOG.isInfoEnabled()) {
        LOG.info(RESOURCE_NOT_FOUND);
      }
    }

    return retryNeeded;
  }

  /**
   * Insufficient instance capacity error code.
   */
  public static final String INSUFFICIENT_INSTANCE_CAPACITY = "InsufficientInstanceCapacity";

  /**
   * Instance limit exceeded error code.
   */
  public static final String INSTANCE_LIMIT_EXCEEDED = "InstanceLimitExceeded";

  /**
   * Request limit exceeded error code..
   */
  public static final String REQUEST_LIMIT_EXCEEDED = "RequestLimitExceeded";

  /**
   * Volume limit exceeded error code.
   */
  public static final String VOLUME_LIMIT_EXCEEDED = "Client.VolumeLimitExceeded";

  /**
   * Internal error error code.
   */
  public static final String INTERNAL_ERROR = "InternalError";


  // The set of error codes representing authorization failures.
  public static final Set<String> AUTHORIZATION_ERROR_CODES = ImmutableSet.of(
      "AuthFailure",
      "UnauthorizedOperation"
  );


  /**
   * Parses a set of exceptions and a set of failed state reasons and throws
   * an appropriate plugin exception with appropriate plugin exception details.
   * This will throw an {@link UnrecoverableProviderException} if the exception
   * set contains any exceptions that are unrecoverable. An UnrecoverableProviderException
   * will also be thrown if the set of failed state reason is not empty. In other cases a
   * {@link TransientProviderException} is thrown.
   *
   * @param message            the plugin exception message to set
   * @param exceptions         a set of exceptions
   * @param failedStateReasons state reasons for instances that transitioned to terminated
   * @param template           the EC2 instance template
   */
  public static void propagate(String message, Set<Exception> exceptions,
      Set<StateReason> failedStateReasons, EC2InstanceTemplate template) {
    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();

    boolean isUnrecoverable = addErrors(template, exceptions, accumulator);

    if (!failedStateReasons.isEmpty()) {
      isUnrecoverable = true;
      addStateReasonErrors(template, failedStateReasons, accumulator);
    }

    PluginExceptionDetails pluginExceptionDetails =
        new PluginExceptionDetails(accumulator.getConditionsByKey());

    if (isUnrecoverable) {
      throw new UnrecoverableProviderException(message, pluginExceptionDetails);
    }

    throw new TransientProviderException(message, pluginExceptionDetails);
  }

  /**
   * Propagates exception as an unrecoverable error when the relevant
   * indicators are present.
   *
   * @param e the Amazon client exception
   */
  public static void propagateIfUnrecoverable(AmazonClientException e) {
    if (isUnrecoverable(e)) {
      if (e instanceof AmazonServiceException) {
        AmazonServiceException ase = (AmazonServiceException) e;
        if (AUTHORIZATION_ERROR_CODES.contains(ase.getErrorCode())) {
          throw new InvalidCredentialsException(ase.getErrorMessage(), ase);
        }
      }
      throw new UnrecoverableProviderException(e.getMessage(), e);
    }
  }

  /**
   * Returns whether the specified throwable is unrecoverable, treating any throwable that is
   * not an Amazon client exception to be unrecoverable.
   *
   * @param t the throwable
   * @return whether the specified throwable is unrecoverable
   */
  public static boolean isUnrecoverable(Throwable t) {
    return !(t instanceof AmazonClientException) || isUnrecoverable((AmazonClientException) t);
  }

  /**
   * Returns whether the specified Amazon client exception is unrecoverable, considering a few
   * categories of exception as unrecoverable in addition to those which are flagged as not
   * retryable by Amazon.
   *
   * @param e the Amazon client exception
   * @return whether the specified Amazon client exception is unrecoverable
   */
  public static boolean isUnrecoverable(AmazonClientException e) {
    if (e instanceof AmazonServiceException) {
      AmazonServiceException ase = (AmazonServiceException) e;
      if (AUTHORIZATION_ERROR_CODES.contains(ase.getErrorCode())) {
        return true;
      }

      // All exceptions that represent client errors are unrecoverable because the request itself is wrong
      // See {@see AmazonServiceException#ErrorType}
      // * OperationNotPermitted exception is unrecoverable. This can happen when terminating an
      //   instance that has termination protection enabled, or trying to detach the primary
      //   network interface (eth0) from an instance.
      // * Unsupported exception is also unrecoverable, since it represents an unsupported request.
      //   See docs at http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
      // * InvalidParameterValue is unrecoverable, as one of the parameters supplied by the user is invalid.
      // * PendingVerification is unrecoverable since it can take up to 2 hours for verification to complete.
      if (ase.getErrorType() == AmazonServiceException.ErrorType.Client ||
          "OperationNotPermitted".equals(ase.getErrorCode()) ||
          "Unsupported".equals(ase.getErrorCode()) ||
          "InvalidParameterValue".equals(ase.getErrorCode()) ||
          "PendingVerification".equals(ase.getErrorCode())) {
        return true;
      }

      // Consider instance limits, insufficient capacity, and internal error as unrecoverable
      if (INSTANCE_LIMIT_EXCEEDED.equals(ase.getErrorCode()) ||
          INSUFFICIENT_INSTANCE_CAPACITY.equals(ase.getErrorCode()) ||
          INTERNAL_ERROR.equals(ase.getErrorCode())) {
        return true;
      }
    }

    return !e.isRetryable();
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
   * Returns whether the specified throwable is an {@code AmazonServiceException} with
   * the specified error code.
   *
   * @param throwable the throwable
   * @param errorCode the error code
   * @return whether the specified throwable is an {@code AmazonServiceException} with
   * the specified error code
   */
  public static boolean isAmazonServiceException(Throwable throwable, String errorCode) {
    return (throwable instanceof AmazonServiceException)
        && errorCode.equals(((AmazonServiceException) throwable).getErrorCode());
  }

  /**
   * Converts a set of exceptions to plugin error conditions. These conditions
   * will be added to the provided accumulator. Returns whether an unrecoverable
   * error is present in provided exceptions.
   *
   * @param template    the EC2 instance template
   * @param exceptions  the list of encountered exceptions
   * @param accumulator the exception condition accumulator to add to
   * @return whether an unrecoverable exception is present
   */
  private static boolean addErrors(EC2InstanceTemplate template,
      Set<Exception> exceptions,
      PluginExceptionConditionAccumulator accumulator) {
    boolean hasUnrecoverableExceptions = false;

    // Only report each error code once
    Map<String, AmazonServiceException> awsExceptions = Maps.newHashMap();
    for (Exception e : exceptions) {
      hasUnrecoverableExceptions = hasUnrecoverableExceptions || isUnrecoverable(e);
      if (e instanceof AmazonServiceException) {
        AmazonServiceException awsEx = (AmazonServiceException) e;
        awsExceptions.put(awsEx.getErrorCode(), awsEx);
      } else {
        accumulator.addError(toExceptionInfoMap(e.getMessage(), "N/A", "N/A"));
      }
    }

    for (AmazonServiceException awsException : awsExceptions.values()) {
      accumulator.addError(toExceptionInfoMap(template, awsException));
    }

    return hasUnrecoverableExceptions;
  }

  private static void addStateReasonErrors(EC2InstanceTemplate template,
      Set<StateReason> stateReasons,
      PluginExceptionConditionAccumulator accumulator) {
    for (StateReason stateReason : stateReasons) {
      String stateReasonCode = stateReason.getCode();
      String stateReasonMessage = stateReason.getMessage();

      String message = "Instance(s) were unexpectedly terminated";
      if (stateReasonCode.equals(VOLUME_LIMIT_EXCEEDED)) {
        message = String.format("Instance(s) were terminated due to volume limits for %s volume type",
            template.getEbsVolumeType());
      }

      accumulator.addError(toExceptionInfoMap(message, stateReasonCode, stateReasonMessage));
    }
  }

  private static Map<String, String> toExceptionInfoMap(EC2InstanceTemplate template,
      AmazonServiceException ex) {
    String awsErrorCode = ex.getErrorCode();
    String message = "Encountered AWS exception";

    // give a clearer message on more common AWS exceptions
    switch (awsErrorCode) {
      case INSUFFICIENT_INSTANCE_CAPACITY:
        message = String.format("AWS does not have available capacity for instance type %s",
            template.getType());
        break;
      case INSTANCE_LIMIT_EXCEEDED:
        message = String.format("Exceeded instance limit for instance type %s",
            template.getType());
        break;
      case REQUEST_LIMIT_EXCEEDED:
        message = "API request limit exceeded";
        break;
      default:
        break;
    }

    return toExceptionInfoMap(message, ex.getErrorCode(), ex.getErrorMessage());
  }

  private static Map<String, String> toExceptionInfoMap(String message, String awsErrorCode,
      String awsErrorMessage) {
    return ImmutableMap.of(
        "message", message,
        "awsErrorCode", awsErrorCode,
        "awsErrorMessage", awsErrorMessage
    );
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private AWSExceptions() {
  }
}
