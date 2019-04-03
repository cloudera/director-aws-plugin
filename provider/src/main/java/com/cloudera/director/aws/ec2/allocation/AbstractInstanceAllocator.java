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

import static com.cloudera.director.aws.ec2.EC2Retryer.retryUntil;
import static com.google.common.collect.Iterables.getOnlyElement;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceBlockDeviceMapping;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClient;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.github.rholder.retry.RetryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for instance allocators.
 */
public abstract class AbstractInstanceAllocator implements InstanceAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractInstanceAllocator.class);

  /**
   * The default wait until instances are started, in milliseconds.
   */
  private static final long DEFAULT_INSTANCE_WAIT_UNTIL_STARTED_MS = 30 * 60 * 1000; //30 min

  /**
   * The configuration key for the wait until instances are started.
   */
  private static final String INSTANCE_WAIT_UNTIL_STARTED_MS =
      "ec2.instance.waitUntilStartedMilliseconds";

  /**
   * The default wait until tagged instances are findable, in milliseconds.
   */
  private static final long DEFAULT_INSTANCE_WAIT_UNTIL_FINDABLE_MS = 10 * 60 * 1000; //10 min

  /**
   * The configuration key for the wait until instances are findable.
   */
  private static final String INSTANCE_WAIT_UNTIL_FINDABLE_MS =
      "ec2.instance.waitUntilFindableMilliseconds";

  /**
   * Returns the error message for the specified exception.
   *
   * @param e the exception
   * @return the error message for the specified exception
   */
  protected static String getErrorMessage(Exception e) {
    String message = e.getMessage();
    return (message == null) ? e.getClass().getSimpleName() : message;
  }

  /**
   * The allocation helper.
   */
  protected final AllocationHelper allocationHelper;

  /**
   * The EC2 client.
   */
  protected final AmazonEC2AsyncClient ec2Client;

  /**
   * The STS client.
   */
  protected final AWSSecurityTokenServiceAsyncClient stsClient;

  /**
   * The tag helper.
   */
  protected final EC2TagHelper ec2TagHelper;

  /**
   * How long to wait for instances to start, in milliseconds.
   */
  protected final long waitUntilStartedMillis;

  /**
   * How long to wait for tagged instances to be findable, in milliseconds.
   */
  protected final long waitUntilFindableMillis;

  /**
   * Whether to tag EBS volumes.
   */
  private final boolean tagEbsVolumes;

  /**
   * The instance template.
   */
  protected final EC2InstanceTemplate template;

  /**
   * The virtual instance IDs for the created instances.
   */
  protected final Collection<String> virtualInstanceIds;

  /**
   * The minimum number of instances to allocate if not all resources can be allocated.
   */
  protected final int minCount;

  /**
   * Creates an abstract instance allocator with the specified parameters.
   *
   * @param allocationHelper   the allocation helper
   * @param ec2Client          the EC2 client
   * @param stsClient          the STS client
   * @param tagEbsVolumes      whether to tag EBS volumes
   * @param template           the instance template
   * @param virtualInstanceIds the virtual instance IDs for the created instances
   * @param minCount           the minimum number of instances to allocate if not all resources
   *                           can be allocated
   */
  public AbstractInstanceAllocator(AllocationHelper allocationHelper,
      AmazonEC2AsyncClient ec2Client, AWSSecurityTokenServiceAsyncClient stsClient, boolean tagEbsVolumes,
      EC2InstanceTemplate template, Collection<String> virtualInstanceIds, int minCount) {
    this.ec2Client = ec2Client;
    this.stsClient = stsClient;
    this.allocationHelper = allocationHelper;
    this.ec2TagHelper = allocationHelper.getEC2TagHelper();

    AWSTimeouts awsTimeouts = allocationHelper.getAWSTimeouts();
    this.waitUntilStartedMillis = awsTimeouts.getTimeout(INSTANCE_WAIT_UNTIL_STARTED_MS)
        .or(DEFAULT_INSTANCE_WAIT_UNTIL_STARTED_MS);
    this.waitUntilFindableMillis = awsTimeouts.getTimeout(INSTANCE_WAIT_UNTIL_FINDABLE_MS)
        .or(DEFAULT_INSTANCE_WAIT_UNTIL_FINDABLE_MS);

    this.tagEbsVolumes = tagEbsVolumes;

    this.template = template;
    this.virtualInstanceIds = virtualInstanceIds;
    this.minCount = minCount;
  }

  @Override
  @VisibleForTesting
  public abstract Collection<EC2Instance> allocate() throws InterruptedException;

  @Override
  @VisibleForTesting
  public Collection<String> getInstanceIds() {
    return virtualInstanceIds;
  }

  @Override
  public abstract void delete() throws InterruptedException;

  /**
   * Determines the idempotency client token for the specified virtual instance ID.
   *
   * @param virtualInstanceId the virtual instance ID
   * @param discriminator     a discriminator to further identify this request
   * @return the idempotency token
   */
  protected String determineClientToken(String virtualInstanceId, Long discriminator) {
    // Using MD5 because clientToken should be less than 64 characters long
    Hasher hasher = Hashing.md5().newHasher(virtualInstanceIds.size());

    hasher.putString(virtualInstanceId, Charsets.UTF_8);
    hasher.putLong(discriminator);
    return hasher.hash().toString();
  }

  /**
   * Waits until all of the specified instances have assigned private IP addresses.
   *
   * @param virtualInstanceIdToEC2InstanceIds a virtual instance Id to EC2 instance Id map
   * @return a virtual instance to instance map in which instance is running and assigned with private IP
   * @throws InterruptedException if the operation is interrupted
   */
  protected Map<String, Instance> waitForPrivateIpAddresses(
      Map<String, String> virtualInstanceIdToEC2InstanceIds)
      throws InterruptedException {

    final Map<String, Instance> virtualInstanceIdToInstanceResult =
        Maps.newHashMapWithExpectedSize(virtualInstanceIdToEC2InstanceIds.size());
    final BiMap<String, String> ec2InstanceIdToVirtualInstanceIds = HashBiMap
        .create(virtualInstanceIdToEC2InstanceIds)
        .inverse();

    while (!ec2InstanceIdToVirtualInstanceIds.isEmpty()) {
      LOG.info(">> Waiting for {} instance(s) to get a private IP allocated", ec2InstanceIdToVirtualInstanceIds.size());

      try {
        DescribeInstancesResult result = ec2Client.describeInstances(
            new DescribeInstancesRequest().withInstanceIds(ec2InstanceIdToVirtualInstanceIds.keySet()));

        allocationHelper.forEachInstance(result, instance -> {
          InstanceStateName currentState =
              InstanceStateName.fromValue(instance.getState().getName());

          String ec2InstanceId = instance.getInstanceId();
          if (currentState.equals(InstanceStateName.Terminated) ||
              currentState.equals(InstanceStateName.ShuttingDown)) {
            LOG.info("<< Instance {} has terminated unexpectedly, skipping IP address wait.", ec2InstanceId);
            ec2InstanceIdToVirtualInstanceIds.remove(ec2InstanceId);

          } else if (instance.getPrivateIpAddress() != null) {
            LOG.info("<< Instance {} got IP {}", ec2InstanceId, instance.getPrivateIpAddress());
            virtualInstanceIdToInstanceResult.put(ec2InstanceIdToVirtualInstanceIds.get(ec2InstanceId), instance);
            ec2InstanceIdToVirtualInstanceIds.remove(ec2InstanceId);
          }
          return null;
        });
      } catch (AmazonServiceException e) {
        if (!AWSExceptions.isNotFound(e)) {
          throw e;
        }
      }

      if (!ec2InstanceIdToVirtualInstanceIds.isEmpty()) {
        LOG.info("Waiting 5 seconds until next check, {} instance(s) still don't have an IP",
            ec2InstanceIdToVirtualInstanceIds.size());

        TimeUnit.SECONDS.sleep(5);
      }
    }

    return virtualInstanceIdToInstanceResult;
  }

  /**
   * Tags an EC2 instance. Expects that the instance already exists or is in the process of
   * being created. This may also tag EBS volumes depending on template configurations.
   *
   * @param template          the instance template
   * @param userDefinedTags   the user-defined tags
   * @param virtualInstanceId the virtual instance id
   * @param ec2InstanceId     the EC2 instance id
   * @param timeout           the time point of timeout
   * @return true if the instance was successfully tagged, false otherwise
   * @throws InterruptedException if the operation is interrupted
   */
  protected boolean tagInstance(
      EC2InstanceTemplate template,
      List<Tag> userDefinedTags,
      String virtualInstanceId,
      final String ec2InstanceId,
      DateTime timeout)
      throws InterruptedException {
    LOG.info(">> Tagging instance {} / {}", ec2InstanceId, virtualInstanceId);

    // Wait for the instance to be started. If it is terminating, skip tagging.
    try {
      if (!allocationHelper.waitUntilInstanceHasStarted(ec2InstanceId, timeout)) {
        return false;
      }
    } catch (TimeoutException e) {
      return false;
    }

    final List<Tag> tags = ec2TagHelper.getInstanceTags(template, virtualInstanceId, userDefinedTags);

    LOG.info("Tags: {}", tags);

    try {
      retryUntil(
          (Callable<Void>) () -> {
            LOG.info("Create tags request.");
            ec2Client.createTags(new CreateTagsRequest().withTags(tags).withResources(ec2InstanceId));
            LOG.info("Create tags request complete.");
            return null;
          },
          timeout);
    } catch (RetryException e) {
      LOG.warn("timeout waiting for spot instance {} tagged", ec2InstanceId);
    } catch (ExecutionException e) {
      if (AmazonServiceException.class.isInstance(e.getCause())) {
        throw AWSExceptions.propagate(stsClient, (AmazonServiceException) e.getCause());
      }
      throw new UnrecoverableProviderException(e.getCause());
    }

    // Tag EBS volumes if they were part of instance launch request
    if (tagEbsVolumes) {
      tagEbsVolumes(ec2InstanceId, template, virtualInstanceId, tags, timeout);
    }

    return true;
  }

  private void tagEbsVolumes(
      final String ec2InstanceId,
      EC2InstanceTemplate template,
      String virtualInstanceId,
      List<Tag> tags,
      DateTime timeout)
      throws InterruptedException {
    DescribeInstancesResult result;
    try {
      result = retryUntil(
          () -> {
            DescribeInstancesRequest request = new DescribeInstancesRequest()
                .withInstanceIds(Collections.singletonList(ec2InstanceId));
            return ec2Client.describeInstances(request);
          },
          timeout);
    } catch (RetryException e) {
      LOG.warn("timeout describing instance {}", ec2InstanceId);
      return;
    } catch (ExecutionException e) {
      if (AmazonServiceException.class.isInstance(e.getCause())) {
        throw AWSExceptions.propagate(stsClient, (AmazonServiceException) e.getCause());
      }
      throw new UnrecoverableProviderException(e.getCause());
    }

    List<InstanceBlockDeviceMapping> instanceBlockDeviceMappings =
        getOnlyElement(getOnlyElement(result.getReservations()).getInstances()).getBlockDeviceMappings();
    for (InstanceBlockDeviceMapping instanceBlockDeviceMapping : instanceBlockDeviceMappings) {
      String volumeId = instanceBlockDeviceMapping.getEbs().getVolumeId();
      tagEbsVolume(template, tags, virtualInstanceId, volumeId);
    }
  }

  private void tagEbsVolume(
      EC2InstanceTemplate template, List<Tag> userDefinedTags, String virtualInstanceId, String volumeId)
      throws InterruptedException {
    LOG.info(">> Tagging volume {} / {}", volumeId, virtualInstanceId);
    List<Tag> tags = ec2TagHelper.getInstanceTags(template, virtualInstanceId, userDefinedTags);
    ec2Client.createTags(new CreateTagsRequest().withTags(tags).withResources(volumeId));
  }
}
