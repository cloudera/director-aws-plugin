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

package com.cloudera.director.aws.ec2.allocation.asg;

import static com.cloudera.director.aws.AWSExceptions.isAmazonServiceException;
import static com.cloudera.director.aws.AWSExceptions.isUnrecoverable;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient;
import com.amazonaws.services.autoscaling.model.AlreadyExistsException;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.LaunchTemplateSpecification;
import com.amazonaws.services.autoscaling.model.SuspendProcessesRequest;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CreateLaunchTemplateRequest;
import com.amazonaws.services.ec2.model.DeleteLaunchTemplateRequest;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.amazonaws.services.ec2.model.LaunchTemplateBlockDeviceMappingRequest;
import com.amazonaws.services.ec2.model.LaunchTemplateEbsBlockDeviceRequest;
import com.amazonaws.services.ec2.model.LaunchTemplateIamInstanceProfileSpecificationRequest;
import com.amazonaws.services.ec2.model.LaunchTemplateInstanceNetworkInterfaceSpecificationRequest;
import com.amazonaws.services.ec2.model.LaunchTemplatePlacementRequest;
import com.amazonaws.services.ec2.model.RequestLaunchTemplateData;
import com.amazonaws.services.ec2.model.Tag;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.common.Callables2;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.cloudera.director.aws.ec2.allocation.AllocationHelper;
import com.cloudera.director.aws.ec2.allocation.InstanceAllocator;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Holds state and logic for allocating an Auto Scaling group. A new instance is required for
 * each allocation request.</p>
 * Atomically allocates an EC2 Auto Scaling Group based on a single instance template.
 * <p>The {@link #allocate()} method atomically allocates an EC2 Auto Scaling Group based on a
 * single configured instance template.</p>
 * TODO determine whether we need a resource leak disclaimer
 */
@SuppressWarnings({"Guava", "OptionalUsedAsFieldOrParameterType"})
public class AutoScalingGroupAllocator implements InstanceAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(AutoScalingGroupAllocator.class);

  /**
   * The default Auto Scaling group request duration, in milliseconds.
   */
  private static final long DEFAULT_AUTO_SCALING_GROUP_REQUEST_DURATION_MS = 10 * 60 * 1000; //10 min

  /**
   * The key for Auto Scaling group timeouts.
   */
  @VisibleForTesting
  static final String AUTO_SCALING_GROUP_REQUEST_DURATION_MS = "ec2.asg.requestDurationMilliseconds";

  /**
   * The default Auto Scaling group instance polling time, in milliseconds.
   */
  private static final long DEFAULT_AUTO_SCALING_GROUP_INSTANCE_POLL_DURATION_MS = 1000; //1 sec

  /**
   * The key for Auto Scaling group instance polling time.
   */
  @VisibleForTesting
  static final String AUTO_SCALING_GROUP_INSTANCE_POLL_DURATION_MS = "ec2.asg.instancePollDurationMilliseconds";

  /**
   * The AWS Auto Scaling process responsible for replacing unhealthy instances.
   */
  @VisibleForTesting
  public static final String SCALING_PROCESS_REPLACE_UNHEALTHY = "ReplaceUnhealthy";

  /**
   * The AWS Auto Scaling process responsible for rebalancing availability zones.
   */
  @VisibleForTesting
  public static final String SCALING_PROCESS_AZ_REBALANCE = "AZRebalance";

  /**
   * The error code that indicates that a launch template was not found.
   */
  private static final String LAUNCH_TEMPLATE_NOT_FOUND_EXCEPTION =
      "InvalidLaunchTemplateName.NotFoundException";

  /**
   * The allocation helper.
   */
  private final AllocationHelper allocationHelper;

  /**
   * The EC2 client.
   */
  private final AmazonEC2AsyncClient ec2Client;

  /**
   * The Auto Scaling client.
   */
  private final AmazonAutoScalingAsyncClient autoScalingClient;

  /**
   * The EC2 tag helper.
   */
  private final EC2TagHelper ec2TagHelper;

  /**
   * The instance template.
   */
  private final EC2InstanceTemplate template;

  /**
   * The desired number of instances in the group.
   */
  private final int desiredCount;

  /**
   * The minimum number of instances in the group.
   */
  private final int minCount;

  /**
   * The latest time to wait for group creation.
   */
  private final Date requestExpirationTime;

  /**
   * The polling duration when waiting for instances to appear in the group.
   */
  private final long instancePollDuration;

  /**
   * The instance group ID.
   */
  private final String groupId;

  /**
   * The launch template name.
   */
  private final String launchTemplateName;

  /**
   * The Auto Scaling Group name.
   */
  private final String autoScalingGroupName;

  /**
   * Creates an Auto Scaling group allocator with the specified parameters.
   *
   * @param allocationHelper  the allocation helper
   * @param ec2Client         the EC2 client
   * @param autoScalingClient the Auto Scaling client
   * @param template          the instance template
   * @param desiredCount      the desired number of instances in the group
   * @param minCount          the minimum number of instances in the group
   */
  public AutoScalingGroupAllocator(AllocationHelper allocationHelper,
      AmazonEC2AsyncClient ec2Client, AmazonAutoScalingAsyncClient autoScalingClient,
      EC2InstanceTemplate template, int desiredCount, int minCount) {
    this.allocationHelper = allocationHelper;
    this.ec2Client = ec2Client;
    this.autoScalingClient = autoScalingClient;
    this.ec2TagHelper = allocationHelper.getEC2TagHelper();

    long requestDuration = allocationHelper.getAWSTimeouts()
        .getTimeout(AUTO_SCALING_GROUP_REQUEST_DURATION_MS)
        .or(DEFAULT_AUTO_SCALING_GROUP_REQUEST_DURATION_MS);
    instancePollDuration = allocationHelper.getAWSTimeouts()
        .getTimeout(AUTO_SCALING_GROUP_INSTANCE_POLL_DURATION_MS)
        .or(DEFAULT_AUTO_SCALING_GROUP_INSTANCE_POLL_DURATION_MS);

    this.template = template;
    this.desiredCount = desiredCount;
    this.minCount = minCount;

    long startTime = System.currentTimeMillis();
    this.requestExpirationTime = new Date(startTime + requestDuration);

    groupId = template.getGroupId();
    launchTemplateName = groupId;
    autoScalingGroupName = groupId;
  }

  /**
   * Allocates an Auto Scaling group.
   *
   * @throws InterruptedException if the operation is interrupted
   */
  @Override
  public Collection<EC2Instance> allocate() throws InterruptedException {

    LOG.info(">> Requesting Auto Scaling group of {} - {} instances for {}",
        minCount, desiredCount, template);

    try {
      // Create launch template
      RequestLaunchTemplateData launchTemplateData = newRequestLaunchTemplateData(template);
      retryAndPropagate(() -> {
        createLaunchTemplate(launchTemplateData);
        return null;
      }, RuntimeException.class);

      // Create Auto Scaling group
      retryAndPropagate(() -> {
        createAutoScalingGroup();
        return null;
      }, RuntimeException.class, Optional.of(this::isInvalidLaunchTemplateException));

      // Optionally disable automatic instance processing
      if (!template.isEnableAutomaticInstanceProcesing()) {
        retryAndPropagate(() -> {
          disableAutomaticInstanceProcessing();
          return null;
        }, RuntimeException.class);
      }

      // Poll for instances in the group until we get the desired count or run out of time
      Set<String> instanceIds = Sets.newHashSet();
      do {
        retryAndPropagate(() -> {
          instanceIds.addAll(getInstanceIds());
          return null;
        }, RuntimeException.class);

        if ((instanceIds.size() >= desiredCount)
            || (System.currentTimeMillis() >= requestExpirationTime.getTime())) {
          break;
        }

        Thread.sleep(instancePollDuration);
      } while (true);

      if (instanceIds.size() < minCount) {
        throw new UnrecoverableProviderException("Only allocated " + instanceIds.size()
            + " of " + minCount + " instances in configured time. Cleaning up resources.");
      }

      return allocationHelper.find(template, instanceIds);
    } catch (RuntimeException e) {
      // TODO revisit cleanup behavior based on min count semantics, and review catch blocks
      // to see if we want to catch Error, InterruptedException, etc.
      try {
        retryAndPropagate(() -> {
          doDelete();
          return null;
        }, RuntimeException.class);
      } catch (Exception e1) {
        LOG.warn("Exception deleting resources while allocating Auto Scaling group: "
            + autoScalingGroupName
            + ". Check AWS console to avoid resource leak.", e1);
        e.addSuppressed(e1);
      }
      AWSExceptions.propagate("Problem allocating Auto Scaling group.",
          toSet(e),
          Collections.emptySet(),
          template
      );
    }
    return Collections.emptyList();
  }

  /**
   * Deletes an Auto Scaling group and associated launch template.
   *
   * @throws InterruptedException if the operation is interrupted
   */
  public void delete() throws InterruptedException {
    try {
      retryAndPropagate(() -> {
        doDelete();
        return null;
      }, RuntimeException.class);
    } catch (RuntimeException e) {
      AWSExceptions.propagate("Problem deleting Auto Scaling group.",
          toSet(e),
          Collections.emptySet(),
          template
      );
    }
  }

  /**
   * Deletes an Auto Scaling group and associated launch template.
   *
   * @throws InterruptedException if the operation is interrupted
   */
  private void doDelete()
      throws InterruptedException {
    Callables2.callAll(
        () -> {
          // Delete Auto Scaling group
          deleteAutoScalingGroup();
          return null;
        },
        () -> {
          // Delete launch template
          deleteLaunchTemplate();
          return null;
        }
    );
  }

  /**
   * Builds a {@code RequestLaunchTemplateData} starting from a template.
   *
   * @param template the instance template
   * @return the request launch template data
   */
  private RequestLaunchTemplateData newRequestLaunchTemplateData(EC2InstanceTemplate template) {
    String image = template.getImage();
    String type = template.getType();

    List<BlockDeviceMapping> deviceMappings = allocationHelper.getBlockDeviceMappings(template);

    // Convert EC2 block device mappings to ASG block device mappings
    List<LaunchTemplateBlockDeviceMappingRequest> autoScalingBlockDeviceMappings =
        new ArrayList<>(deviceMappings.size());
    for (BlockDeviceMapping deviceMapping : deviceMappings) {
      LaunchTemplateBlockDeviceMappingRequest launchTemplateBlockDeviceMapping =
          new LaunchTemplateBlockDeviceMappingRequest()
              .withDeviceName(deviceMapping.getDeviceName())
              .withVirtualName(deviceMapping.getVirtualName());
      EbsBlockDevice ebsBlockDevice = deviceMapping.getEbs();
      if (ebsBlockDevice != null) {
        LaunchTemplateEbsBlockDeviceRequest ebs = new LaunchTemplateEbsBlockDeviceRequest()
            .withVolumeType(ebsBlockDevice.getVolumeType())
            .withVolumeSize(ebsBlockDevice.getVolumeSize())
            .withEncrypted(ebsBlockDevice.getEncrypted())
            .withKmsKeyId(ebsBlockDevice.getKmsKeyId())
            .withDeleteOnTermination(ebsBlockDevice.getDeleteOnTermination());
        launchTemplateBlockDeviceMapping = launchTemplateBlockDeviceMapping
            .withEbs(ebs);
      }
      String noDevice = deviceMapping.getNoDevice();
      if (noDevice != null) {
        launchTemplateBlockDeviceMapping = launchTemplateBlockDeviceMapping
            .withNoDevice(noDevice);
      }
      autoScalingBlockDeviceMappings.add(
          launchTemplateBlockDeviceMapping);
    }

    LaunchTemplateInstanceNetworkInterfaceSpecificationRequest network =
        getLaunchTemplateInstanceNetworkInterfaceSpecification(template);

    LaunchTemplatePlacementRequest placement = new LaunchTemplatePlacementRequest()
        .withTenancy(template.getTenancy());

    LOG.info(">> Auto Scaling group request type: {}, image: {}", type, image);

    RequestLaunchTemplateData request = new RequestLaunchTemplateData()
        .withImageId(image)
        .withInstanceType(type)
        .withNetworkInterfaces(network)
        .withBlockDeviceMappings(autoScalingBlockDeviceMappings)
        .withEbsOptimized(template.isEbsOptimized())
        .withPlacement(placement);

    if (template.getIamProfileName().isPresent()) {
      LaunchTemplateIamInstanceProfileSpecificationRequest iamInstanceProfile =
          new LaunchTemplateIamInstanceProfileSpecificationRequest()
              .withName(template.getIamProfileName().get());
      request = request.withIamInstanceProfile(iamInstanceProfile);
    }

    if (template.getKeyName().isPresent()) {
      request.withKeyName(template.getKeyName().get());
    }

    Optional<String> userData = template.getUserData();
    if (userData.isPresent()) {
      request.withUserData(userData.get());
    }

    return request;
  }

  /**
   * Creates a launch template instance network interface specification based on the specified
   * instance template.
   *
   * @param template the instance template
   * @return launch template instance network interface specification
   */
  private LaunchTemplateInstanceNetworkInterfaceSpecificationRequest
  getLaunchTemplateInstanceNetworkInterfaceSpecification(EC2InstanceTemplate template) {
    LaunchTemplateInstanceNetworkInterfaceSpecificationRequest network =
        new LaunchTemplateInstanceNetworkInterfaceSpecificationRequest()
            .withDeviceIndex(0)
            .withSubnetId(template.getSubnetId())
            .withGroups(template.getSecurityGroupIds())
            .withDeleteOnTermination(true);

    LOG.info(">> Launch template network interface specification: {}", network);
    return network;
  }

  /**
   * Creates a launch template from the configured launch template data.
   *
   * @param launchTemplateData the launch template data
   * @throws InterruptedException if the operation is interrupted
   */
  private void createLaunchTemplate(RequestLaunchTemplateData launchTemplateData)
      throws InterruptedException {
    CreateLaunchTemplateRequest createLaunchTemplateRequest =
        new CreateLaunchTemplateRequest()
            .withLaunchTemplateName(launchTemplateName)
            .withLaunchTemplateData(launchTemplateData);
    //.withClientToken(template.getGroupId() + launchTemplateData.hashCode()); // TODO figure out client token
    createLaunchTemplate(createLaunchTemplateRequest);
  }

  /**
   * Creates an Auto Scaling launch template from the specified request.
   *
   * @param request the create launch template request
   * @throws InterruptedException if the operation is interrupted
   */
  private void createLaunchTemplate(CreateLaunchTemplateRequest request)
      throws InterruptedException {
    LOG.info(">> Creating launch template: " + launchTemplateName);
    try {
      ec2Client.createLaunchTemplate(request);
    } catch (AlreadyExistsException ignore) {
    } catch (AmazonServiceException ase) {
      // TODO update existing group?
      if (!"InvalidLaunchTemplateName.AlreadyExistsException".equals(ase.getErrorCode())) {
        throw ase;
      }
    }
  }

  /**
   * Deletes the configured launch template.
   */
  private void deleteLaunchTemplate() {
    DeleteLaunchTemplateRequest deleteLaunchTemplateRequest =
        new DeleteLaunchTemplateRequest()
            .withLaunchTemplateName(launchTemplateName);
    try {
      ec2Client.deleteLaunchTemplate(deleteLaunchTemplateRequest);
    } catch (RuntimeException e) {
      // OK if launch template does not exist
      if (!isInvalidLaunchTemplateException(e)) {
        throw e;
      }
    }
  }

  /**
   * Creates an Auto Scaling Group from the configured template and Auto Scaling Group name.
   *
   * @throws InterruptedException if the operation is interrupted
   */
  private void createAutoScalingGroup()
      throws InterruptedException {
    LOG.info(">> Creating Auto Scaling group: " + autoScalingGroupName);
    List<Tag> userDefinedTags = ec2TagHelper.getUserDefinedTags(template);
    CreateAutoScalingGroupRequest createAutoScalingGroupRequest =
        newCreateAutoScalingGroupRequest(template, userDefinedTags);
    try {
      autoScalingClient.createAutoScalingGroup(createAutoScalingGroupRequest);
    } catch (AlreadyExistsException ignore) {
    } catch (AmazonServiceException ase) {
      // TODO update existing group?
      if (!"AlreadyExists".equals(ase.getErrorCode())) {
        throw ase;
      }
    }
  }

  /**
   * Builds a {@code CreateAutoScalingGroupRequest} starting from a template.
   *
   * @param template        the instance template
   * @param userDefinedTags user defined tags to attach to the group
   * @return the Auto Scaling group request
   * @throws InterruptedException if the operation is interrupted
   */
  @SuppressWarnings("ConstantConditions")
  private CreateAutoScalingGroupRequest newCreateAutoScalingGroupRequest(
      EC2InstanceTemplate template, List<Tag> userDefinedTags)
      throws InterruptedException {

    List<Tag> tags = ec2TagHelper.getInstanceTags(template, groupId, userDefinedTags);
    List<com.amazonaws.services.autoscaling.model.Tag> autoScalingGroupTags =
        new ArrayList<>(tags.size());
    for (Tag tag : tags) {
      autoScalingGroupTags.add(new com.amazonaws.services.autoscaling.model.Tag()
          .withResourceType("auto-scaling-group")
          .withResourceId(groupId)
          .withKey(tag.getKey())
          .withValue(tag.getValue())
          .withPropagateAtLaunch(true));
    }

    LaunchTemplateSpecification launchTemplateSpecification = new LaunchTemplateSpecification()
        .withLaunchTemplateName(launchTemplateName);

    CreateAutoScalingGroupRequest request = new CreateAutoScalingGroupRequest()
        .withAutoScalingGroupName(autoScalingGroupName)
        .withDesiredCapacity(desiredCount)
        .withLaunchTemplate(launchTemplateSpecification)
        .withVPCZoneIdentifier(template.getSubnetId())
        .withMinSize(minCount)
        .withMaxSize(desiredCount)
        .withTags(autoScalingGroupTags);

    if (template.getAvailabilityZone().isPresent()) {
      request = request.withAvailabilityZones(template.getAvailabilityZone().get());
    }
    if (template.getPlacementGroup().isPresent()) {
      request = request.withPlacementGroup(template.getPlacementGroup().get());
    }

    return request;
  }

  /**
   * Deletes the configured Auto Scaling Group.
   */
  private void deleteAutoScalingGroup() {
    // TODO Add live test for idempotency, see if we need exception handling here
    DeleteAutoScalingGroupRequest deleteAutoScalingGroupRequest =
        new DeleteAutoScalingGroupRequest()
            .withAutoScalingGroupName(autoScalingGroupName)
            .withForceDelete(true);
    autoScalingClient.deleteAutoScalingGroup(deleteAutoScalingGroupRequest);
  }

  /**
   * Returns the instance IDs of the instances in the Auto Scaling Group.
   *
   * @return the instance IDs of the instances in the Auto Scaling Group
   */
  private List<String> getInstanceIds() {
    DescribeAutoScalingGroupsRequest describeAutoScalingGroupsRequest =
        new DescribeAutoScalingGroupsRequest()
        .withAutoScalingGroupNames(autoScalingGroupName);
    DescribeAutoScalingGroupsResult describeAutoScalingGroupsResult =
        autoScalingClient.describeAutoScalingGroups(describeAutoScalingGroupsRequest);
    // The result will contain at most one Auto Scaling Group, so no need to loop over next tokens
    List<AutoScalingGroup> autoScalingGroups = describeAutoScalingGroupsResult.getAutoScalingGroups();
    return autoScalingGroups.stream()
        .flatMap((g) -> g.getInstances().stream())
        .map(com.amazonaws.services.autoscaling.model.Instance::getInstanceId)
        .collect(Collectors.toList());
  }

  /**
   * Disables automatic instance processing for the Auto Scaling group.
   *
   * @throws InterruptedException if the operation is interrupted
   */
  private void disableAutomaticInstanceProcessing()
      throws InterruptedException {
    LOG.info(">> Disabling automatic instance processing for Auto Scaling group: " + autoScalingGroupName);

    SuspendProcessesRequest request = new SuspendProcessesRequest()
        .withAutoScalingGroupName(autoScalingGroupName)
        .withScalingProcesses(SCALING_PROCESS_REPLACE_UNHEALTHY, SCALING_PROCESS_AZ_REBALANCE);
    autoScalingClient.suspendProcesses(request);
  }

  /**
   * Returns whether the specified throwable indicates that a launch template cannot be found.
   *
   * @param throwable the throwable
   * @return whether the specified throwable indicates that a launch template cannot be found
   */
  private boolean isInvalidLaunchTemplateException(Throwable throwable) {
    return isAmazonServiceException(throwable, LAUNCH_TEMPLATE_NOT_FOUND_EXCEPTION);
  }

  /**
   * Retries the specified operation until it succeeds, throws an unrecoverable exception,
   * is interrupted, or exceeds this allocator's configured request expiration time.
   *
   * @param call         the operation to perform
   * @param declaredType the type of exception declared by the operation
   * @param <X>          the type of exception declared by the operation
   * @throws InterruptedException if the operation is interrupted
   * @throws X                    if the declared exception occurs
   */
  private <X extends Throwable> void retryAndPropagate(Callable<Void> call, Class<X> declaredType)
      throws InterruptedException, X {
    retryAndPropagate(call, declaredType, Optional.absent());
  }

  /**
   * Retries the specified operation until it succeeds, throws an unrecoverable exception,
   * is interrupted, or exceeds this allocator's configured request expiration time.
   *
   * @param call                   the operation to perform
   * @param declaredType           the type of exception declared by the operation
   * @param additionalRetryMatcher an optional matcher for exceptions that would normally be
   *                               considered unrecoverable but should be retried
   * @param <X>                    the type of exception declared by the operation
   * @throws InterruptedException if the operation is interrupted
   * @throws X                    if the declared exception occurs
   */
  private <X extends Throwable> void retryAndPropagate(Callable<Void> call, Class<X> declaredType,
      Optional<Predicate<Throwable>> additionalRetryMatcher)
      throws InterruptedException, X {
    // TODO see if the differences between this behavior and EC2Retryer can be bridged.
    long delay = Math.max(requestExpirationTime.getTime() - System.currentTimeMillis(), 1L);
    Retryer<Void> retryer = RetryerBuilder.<Void>newBuilder()
        .retryIfException((t) -> !isUnrecoverable(t)
            || (additionalRetryMatcher.isPresent() && additionalRetryMatcher.get().apply(t)))
        .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(delay, TimeUnit.MILLISECONDS))
        .build();
    try {
      retryer.call(call);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, InterruptedException.class, declaredType);
      throw new IllegalStateException(cause);
    } catch (RetryException e) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, InterruptedException.class, declaredType);
      throw new IllegalStateException(cause);
    }
  }

  /**
   * If the specified throwable or any of its suppressed throwables are Error or InterruptedException,
   * rethrows the first such throwable encountered; otherwise returns a set containing the throwable
   * and all of its suppressed throwables, each cast to Exception.
   *
   * @param throwable a throwable
   * @return a set containing the throwable and all of its suppressed throwables, each cast to Exception,
   * unless it or one of its suppressed throwables is an Error or an InterruptedException
   * @throws InterruptedException if the specified throwable, or any of its suppressed throwables,
   *                              is an InterruptedException
   * @throws Error                if the specified throwable, or any of its suppressed throwables,
   *                              is an Error
   */
  private Set<Exception> toSet(Throwable throwable) throws InterruptedException {
    Throwable[] suppressed = throwable.getSuppressed();
    if (suppressed.length == 0) {
      return Collections.singleton(propagateErrorAndInterruptedException(throwable));
    } else {
      Set<Exception> exceptions = Sets.newHashSet();
      exceptions.add(propagateErrorAndInterruptedException(throwable));
      for (Throwable t : suppressed) {
        exceptions.add(propagateErrorAndInterruptedException(t));
      }
      return exceptions;
    }
  }

  /**
   * Rethrows the specified throwable if it is an Error or InterruptedException; otherwise casts
   * it to Exception and returns it.
   *
   * @param t a throwable
   * @return the specified throwable, cast to Exception, unless it is an Error or an InterruptedException
   * @throws InterruptedException if the specified throwable is an InterruptedException
   * @throws Error                if the specified throwable is an Error
   */
  private Exception propagateErrorAndInterruptedException(Throwable t) throws InterruptedException {
    Throwables.throwIfInstanceOf(t, Error.class);
    Throwables.throwIfInstanceOf(t, InterruptedException.class);
    return (Exception) t;
  }
}
