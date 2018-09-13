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

package com.cloudera.director.aws.ec2.allocation.spot;

import com.google.common.annotations.VisibleForTesting;

import java.net.InetAddress;

/**
 * Holds details about the allocation state of a single virtual instance.
 */
@VisibleForTesting
public class SpotAllocationRecord {

  /**
   * The virtual instance ID.
   */
  @VisibleForTesting
  final String virtualInstanceId;

  /**
   * The Spot instance request ID, or {@code null} if a Spot instance has not been requested.
   */
  @VisibleForTesting
  String spotInstanceRequestId;

  /**
   * The EC2 instance ID, or {@code null} if an instance has not been provisioned.
   */
  @VisibleForTesting
  String ec2InstanceId;

  /**
   * Whether the EC2 instance has been tagged.
   */
  @VisibleForTesting
  boolean instanceTagged;

  /**
   * The private IP address of the EC2 instance, or {@code null} if the instance does not yet
   * have a private IP address.
   */
  @VisibleForTesting
  InetAddress privateIpAddress;

  /**
   * Creates a Spot allocation record with the specified parameters.
   *
   * @param virtualInstanceId the virtual instance ID
   */
  @VisibleForTesting
  public SpotAllocationRecord(String virtualInstanceId) {
    this.virtualInstanceId = virtualInstanceId;
  }
}
