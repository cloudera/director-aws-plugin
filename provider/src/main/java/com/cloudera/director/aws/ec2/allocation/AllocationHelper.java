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

package com.cloudera.director.aws.ec2.allocation;

import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.Tag;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.joda.time.DateTime;

/**
 * Helper that allows instance allocators to delegate operations back to the provider.
 */
@SuppressWarnings("Guava")
public interface AllocationHelper {

  Set<InstanceStateName> UNALLOCATED_STATES = Collections
      .unmodifiableSet(EnumSet.of(InstanceStateName.Terminated, InstanceStateName.ShuttingDown));

  Predicate<Instance> INSTANCE_IS_TERMINAL =
      instance -> instance != null &&
          UNALLOCATED_STATES.contains(InstanceStateName.fromValue(instance.getState().getName()));

  /**
   * Returns the AWS timeouts.
   *
   * @return the AWS timeouts
   */
  AWSTimeouts getAWSTimeouts();

  /**
   * Returns the tag helper.
   *
   * @return the tag helper
   */
  EC2TagHelper getEC2TagHelper();

  /**
   * Creates an EC2 compute instance with the specified parameters.
   *
   * @param template        the template from which the instance was created
   * @param instanceId      the instance identifier
   * @param instanceDetails the provider-specific instance details
   * @throws IllegalArgumentException if the instance does not have a valid private IP address
   */
  EC2Instance createInstance(EC2InstanceTemplate template, String instanceId, Instance instanceDetails);

  /**
   * Waits until the instance has entered a running state.
   *
   * @param ec2InstanceId the EC2 instance id
   * @param timeout       the timeout in milliseconds
   * @return true if the instance has entered a running state, false if the instance is shutting
   * down/terminated or the function has timed out waiting for the instance to enter one of these
   * two states.
   */
  boolean waitUntilInstanceHasStarted(final String ec2InstanceId, DateTime timeout)
      throws InterruptedException, TimeoutException;

  /**
   * Returns current information for the specified instances, which are guaranteed to have
   * been created by this provider.
   *
   * @param template    the template that was used to create the instances
   * @param instanceIds the unique identifiers for the instances
   * @return the instances
   * @throws InterruptedException if the operation is interrupted
   */
  Collection<EC2Instance> find(final EC2InstanceTemplate template, Collection<String> instanceIds)
      throws InterruptedException;

  /**
   * Returns current information for the specified instances, which are guaranteed to have
   * been created by this provider.
   *
   * @param template    the template that was used to create the instances
   * @param instanceIds the unique identifiers for the instances
   * @return the instances
   * @throws InterruptedException if the operation is interrupted
   */
  default Iterable<Map.Entry<String, Instance>> doFind(
      EC2InstanceTemplate template, Iterable<String> instanceIds)
      throws InterruptedException {
    return doFind(template, instanceIds, Predicates.alwaysTrue());
  }

  /**
   * Returns current information for the specified instances, which are guaranteed to have
   * been created by this provider.
   *
   * @param template    the template that was used to create the instances
   * @param instanceIds the unique identifiers for the instances
   * @param predicate   a predicate used to filter the returned instances
   * @return the instances
   * @throws InterruptedException if the operation is interrupted
   */
  Iterable<Map.Entry<String, Instance>> doFind(
      EC2InstanceTemplate template, Iterable<String> instanceIds,
      Predicate<Instance> predicate)
      throws InterruptedException;

  /**
   * Deletes the specified instances, which are guaranteed to have been created by this provider.
   *
   * @param template    the template that was used to create the instances
   * @param instanceIds the unique identifiers for the instances
   * @throws InterruptedException if the operation is interrupted
   */
  void delete(EC2InstanceTemplate template, Collection<String> instanceIds)
      throws InterruptedException;

  /**
   * Deletes the specified EC2 instances.
   *
   * @param ec2InstanceIds the instance IDs
   * @throws InterruptedException if the operation is interrupted
   */
  void doDelete(Collection<String> ec2InstanceIds) throws InterruptedException;

  /**
   * Iterates through the instances in the specified {@code DescribeInstancesResult}
   * and calls the specified handler on each instance. This method will retrieve the
   * follow-on {@code DescribeInstanceResult}s if the result holds a {@code nextToken}.
   *
   * @param result          the {@code DescribeInstancesResult}
   * @param instanceHandler the instance handler
   */
  void forEachInstance(DescribeInstancesResult result, Function<Instance, Void> instanceHandler);

  /**
   * Determines the virtual instance ID from the specified list of tags.
   *
   * @param tags the tags
   * @param type the type of tagged object
   * @return the virtual instance ID
   * @throws IllegalStateException if the tags do not contain the virtual instance ID
   */
  String getVirtualInstanceId(List<Tag> tags, String type);

  /**
   * Creates block device mappings based on the specified instance template.
   *
   * @param template the instance template
   * @return the block device mappings
   */
  List<BlockDeviceMapping> getBlockDeviceMappings(EC2InstanceTemplate template);

  /**
   * Creates an instance network interface specification based on the specified instance template.
   *
   * @param template the instance template
   * @return instance network interface specification
   */
  InstanceNetworkInterfaceSpecification getInstanceNetworkInterfaceSpecification(
      EC2InstanceTemplate template);

  /**
   * Returns whether to associate public IP addresses with the allocated instances.
   *
   * @return whether to associate public IP addresses with the allocated instances
   */
  boolean isAssociatePublicIpAddresses();
}
