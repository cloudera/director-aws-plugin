//  (c) Copyright 2015 Cloudera, Inc.

package com.cloudera.director.aws.ec2;

import com.amazonaws.services.ec2.model.SpotInstanceState;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EC2 Spot instance request status codes.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-bid-status.html#spot-instance-bid-status-understand">Spot Bid Status Codes</a>
 */
public enum SpotInstanceRequestStatusCode {

  /**
   * Indicates that a Spot instance request is pending evaluation.
   */
  PENDING_EVALUATION("pending-evaluation", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because of insufficient
   * capacity.
   */
  CAPACITY_NOT_AVAILABLE("capacity-not-available", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because capacity is
   * oversubscribed.
   */
  CAPACITY_OVERSUBSCRIBED("capacity-oversubscribed", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because the Spot bid is
   * below the current Spot price.
   */
  PRICE_TOO_LOW("price-too-low", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because it has not been
   * scheduled yet.
   */
  NOT_SCHEDULED_YET("not-scheduled-yet", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because a launch group
   * constraint has not yet been satisfied.
   */
  LAUNCH_GROUP_CONSTRAINT("launch-group-constraint", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because an availability group
   * constraint has not yet been satisfied.
   */
  AZ_GROUP_CONSTRAINT("az-group-constraint", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because a placement group
   * constraint has not yet been satisfied.
   */
  PLACEMENT_GROUP_CONSTRAINT("placement-group-constraint", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has not been fulfilled because a constraint has not
   * yet been satisfied.
   */
  CONSTRAINT_NOT_FULFILLABLE("constraint-not-fulfillable", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request is valid, its constraints are satisfied, and its
   * Spot bid is high enough, but the corresponding instance has not yet been provisioned.
   */
  PENDING_FULFILLMENT("pending-fulfillment", SpotInstanceState.Open, false),

  /**
   * Indicates that a Spot instance request has been fulfilled and the corresponding instance
   * is being launched.
   */
  FULFILLED("fulfilled", SpotInstanceState.Active, false),

  /**
   * Indicates that a Spot instance request has been canceled, but the corresponding instance
   * is still running.
   */
  REQUEST_CANCELED_AND_INSTANCE_RUNNING(
      "request-canceled-and-instance-running", SpotInstanceState.Cancelled, false),

  /**
   * Indicates that the instance corresponding to a Spot instance request has been marked
   * for termination.
   */
  MARKED_FOR_TERMINATION("marked-for-termination", SpotInstanceState.Closed, false),

  /**
   * Indicates that Spot instance request parameters were invalid.
   */
  BAD_PARAMETERS("bad-parameters", SpotInstanceState.Failed, true),

  /**
   * Indicates that a Spot instance request was not fulfilled before its expiration time.
   */
  SCHEDULE_EXPIRED("schedule-expired", SpotInstanceState.Closed, true),

  /**
   * Indicates that a Spot instance request was not fulfilled before it was canceled by the user.
   */
  CANCELED_BEFORE_FULFILLMENT("canceled-before-fulfillment", SpotInstanceState.Cancelled, true),

  /**
   * Indicates that a Spot instance request was not fulfilled because of a system error.
   */
  SYSTEM_ERROR("system-error", SpotInstanceState.Closed, true),

  /**
   * Indicates that the instance corresponding to a Spot instance request was terminated because
   * the Spot price rose above the request's Spot bid.
   */
  INSTANCE_TERMINATED_BY_PRICE("instance-terminated-by-price", SpotInstanceState.Closed, true),

  /**
   * Indicates that the instance corresponding to a Spot instance request was terminated by
   * the user.
   */
  INSTANCE_TERMINATED_BY_USER("instance-terminated-by-user", SpotInstanceState.Closed, true),

  /**
   * Indicates that the instance corresponding to a Spot instance request has been terminated
   * because of insufficient capacity.
   */
  INSTANCE_TERMINATED_NO_CAPACITY(
      "instance-terminated-no-capacity", SpotInstanceState.Closed, true),

  /**
   * Indicates that the instance corresponding to a Spot instance request has been terminated
   * because capacity was oversubscribed.
   */
  INSTANCE_TERMINATED_CAPACITY_OVERSUBSCRIBED(
      "instance-terminated-capacity-oversubscribed", SpotInstanceState.Closed, true),

  /**
   * Indicates that the instance corresponding to a Spot instance request has been terminated
   * because of a launch group constraint.
   */
  INSTANCE_TERMINATED_LAUNCH_GROUP_CONSTRAINT(
      "instance-terminated-launch-group-constraint", SpotInstanceState.Closed, true),

  /**
   * Represents an unknown Spot instance request status.
   */
  UNKNOWN("unknown", null, true);

  private static final Logger LOG = LoggerFactory.getLogger(EC2Provider.class);

  /**
   * A map from status code strings to Spot instance request status codes.
   */
  private static final Map<String, SpotInstanceRequestStatusCode>
      SPOT_INSTANCE_STATUS_CODE_BY_STATUS_CODE_STRING;

  static {
    ImmutableMap.Builder<String, SpotInstanceRequestStatusCode> builder = ImmutableMap.builder();
    for (SpotInstanceRequestStatusCode statusCode : values()) {
      builder.put(statusCode.getStatusCodeString(), statusCode);
    }
    SPOT_INSTANCE_STATUS_CODE_BY_STATUS_CODE_STRING = builder.build();
  }

  /**
   * Returns the Spot instance request status code corresponding to the specified status code
   * string, or UNKNOWN if the status code string is not recognized.
   *
   * @param statusCodeString the status code string
   * @return the corresponding Spot instance request status code, or UNKNOWN if the status code
   * string is not recognized
   */
  public static SpotInstanceRequestStatusCode getSpotInstanceStatusCodeByStatusCodeString(
      String statusCodeString) {
    SpotInstanceRequestStatusCode statusCode =
        SPOT_INSTANCE_STATUS_CODE_BY_STATUS_CODE_STRING.get(statusCodeString);
    if (statusCode == null) {
      LOG.warn("Unknown Spot instance request status code {}", statusCodeString);
      statusCode = UNKNOWN;
    }
    return statusCode;
  }

  /**
   * The status code string.
   */
  private final String statusCodeString;

  /**
   * The corresponding Spot instance state.
   */
  private final SpotInstanceState spotInstanceState;

  /**
   * Whether the status is terminal.
   */
  private final boolean terminal;

  /**
   * Creates a Spot instance status code with the specified parameters.
   *
   * @param statusCodeString the status code string
   */
  SpotInstanceRequestStatusCode(String statusCodeString, SpotInstanceState spotInstanceState,
      boolean terminal) {
    this.statusCodeString = statusCodeString;
    this.spotInstanceState = spotInstanceState;
    this.terminal = terminal;
  }

  /**
   * Returns the status code string.
   *
   * @return the status code string
   */
  public String getStatusCodeString() {
    return statusCodeString;
  }

  /**
   * Returns the corresponding Spot instance request state.
   *
   * @return the corresponding Spot instance request state
   */
  public SpotInstanceState getSpotInstanceState() {
    return spotInstanceState;
  }

  /**
   * Returns whether the status is terminal.
   *
   * @return whether the status is terminal
   */
  public boolean isTerminal() {
    return terminal;
  }
}
