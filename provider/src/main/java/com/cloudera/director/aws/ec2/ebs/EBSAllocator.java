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

package com.cloudera.director.aws.ec2.ebs;

import static com.cloudera.director.aws.ec2.EC2Retryer.retryUntil;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.DeleteVolumeRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.EbsInstanceBlockDeviceSpecification;
import com.amazonaws.services.ec2.model.InstanceAttributeName;
import com.amazonaws.services.ec2.model.InstanceBlockDeviceMapping;
import com.amazonaws.services.ec2.model.InstanceBlockDeviceMappingSpecification;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.ec2.model.Volume;
import com.amazonaws.services.ec2.model.VolumeAttachment;
import com.amazonaws.services.ec2.model.VolumeAttachmentState;
import com.amazonaws.services.ec2.model.VolumeState;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.github.rholder.retry.RetryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality to create and attach EBS volumes.
 */
public class EBSAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(EBSAllocator.class);

  private static final long WAIT_UNTIL_AVAILABLE_INTERVAL_SECONDS = 5;
  private static final long WAIT_UNTIL_ATTACHED_INTERVAL_SECONDS = 5;

  /**
   * The key for the timeout to wait for EBS volumes to become available.
   */
  public static final String TIMEOUT_AVAILABLE = "ec2.ebs.availableSeconds";
  /**
   * The key for the timeout to wait for EBS volumes to be attached.
   */
  public static final String TIMEOUT_ATTACH = "ec2.ebs.attachSeconds";

  private static final long DEFAULT_TIMEOUT_SECONDS = 180L;

  private static final String INVALID_STATE_MISSING_KMS_KEY =
      "EBS volumes should only be separately created when they need to be encrypted with a user specified KMS key";

  @VisibleForTesting
  static final String DEVICE_NAME_START_PREFIX = "/dev/sd";

  @VisibleForTesting
  static final char DEVICE_NAME_START_CHAR = 'f';

  private final AmazonEC2Client client;
  private final long availableTimeoutSeconds;
  private final long attachTimeoutSeconds;
  private final EC2TagHelper ec2TagHelper;
  private final EBSDeviceMappings ebsDeviceMappings;

  /**
   * Constructs a new EBS allocator instance.
   *
   * @param client       a pre-configured ec2 client
   * @param awsTimeouts  the AWS timeouts
   * @param ec2TagHelper the custom tag mappings
   */
  public EBSAllocator(AmazonEC2Client client, AWSTimeouts awsTimeouts,
      EC2TagHelper ec2TagHelper, EBSDeviceMappings ebsDeviceMappings) {
    checkNotNull(awsTimeouts, "awsTimeouts is null");

    this.client = checkNotNull(client, "ec2 client is null");
    this.availableTimeoutSeconds =
        awsTimeouts.getTimeout(TIMEOUT_AVAILABLE).or(DEFAULT_TIMEOUT_SECONDS);
    this.attachTimeoutSeconds =
        awsTimeouts.getTimeout(TIMEOUT_ATTACH).or(DEFAULT_TIMEOUT_SECONDS);
    this.ec2TagHelper = checkNotNull(ec2TagHelper, "ec2TagHelper is null");
    this.ebsDeviceMappings = checkNotNull(ebsDeviceMappings, "ebsDeviceMappings is null");
  }

  /**
   * Represents an instance along with the EBS volumes associated with it.
   */
  public static class InstanceEbsVolumes {

    public static final String UNCREATED_VOLUME_ID = "uncreated";

    private final String virtualInstanceId;
    private final String ec2InstanceId;
    private final Map<String, Status> volumeStatuses;

    public enum Status {
      CREATED,
      AVAILABLE,
      ATTACHED,
      FAILED
    }

    /**
     * Constructor.
     *
     * @param virtualInstanceId the Director virtual instance id
     * @param ec2InstanceId     the AWS EC2 instance id
     * @param volumeStatuses    the volume ids for each instance along with the status of the volume
     */
    public InstanceEbsVolumes(String virtualInstanceId, String ec2InstanceId, Map<String, Status> volumeStatuses) {
      this.virtualInstanceId = requireNonNull(virtualInstanceId, "virtualInstanceId is null");
      this.ec2InstanceId = requireNonNull(ec2InstanceId, "ec2InstanceId is null");
      this.volumeStatuses = ImmutableMap.copyOf(volumeStatuses);
    }

    public String getVirtualInstanceId() {
      return virtualInstanceId;
    }

    public String getEc2InstanceId() {
      return ec2InstanceId;
    }

    public Map<String, Status> getVolumeStatuses() {
      return volumeStatuses;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InstanceEbsVolumes that = (InstanceEbsVolumes) o;

      if (!virtualInstanceId.equals(that.virtualInstanceId)) return false;
      if (!ec2InstanceId.equals(that.ec2InstanceId)) return false;
      return volumeStatuses.equals(that.volumeStatuses);

    }

    @Override
    public int hashCode() {
      int result = virtualInstanceId.hashCode();
      result = 31 * result + ec2InstanceId.hashCode();
      result = 31 * result + volumeStatuses.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "InstanceEbsVolumes{" +
          "virtualInstanceId='" + virtualInstanceId + '\'' +
          ", ec2InstanceId='" + ec2InstanceId + '\'' +
          ", volumeStatuses=" + volumeStatuses +
          '}';
    }
  }

  /**
   * Runs create volume requests for each instance. The number and type
   * of volumes to create are taken from the instance template. Volumes are
   * tagged on creation.
   *
   * @param template                          the instance template
   * @param ec2InstanceIdsByVirtualInstanceId ids of the instances where the key is the virtual
   *                                          instance id and the value is the ec2 instance id
   * @return a list of InstanceEbsVolumes
   */
  public List<InstanceEbsVolumes> createVolumes(EC2InstanceTemplate template,
      BiMap<String, String> ec2InstanceIdsByVirtualInstanceId) {
    checkState(template.getEbsKmsKeyId().isPresent(), INVALID_STATE_MISSING_KMS_KEY);

    Optional<String> templateAvailabilityZone = template.getAvailabilityZone();

    String availabilityZone = templateAvailabilityZone.isPresent() ?
        templateAvailabilityZone.get() :
        getAvailabilityZoneFromSubnetId(template.getSubnetId());

    int volumesPerInstance = template.getEbsVolumeCount();
    LOG.info("Requesting {} volumes each for {} instances",
        volumesPerInstance, ec2InstanceIdsByVirtualInstanceId.size());

    int uncreatedVolumeCount = 0;

    // Pre-compute user-defined tags for efficiency
    List<Tag> userDefinedTags = ec2TagHelper.getUserDefinedTags(template);

    List<InstanceEbsVolumes> instanceEbsVolumesList = Lists.newArrayList();
    for (Map.Entry<String, String> entry : ec2InstanceIdsByVirtualInstanceId.entrySet()) {
      String virtualInstanceId = entry.getKey();
      String ec2InstanceId = entry.getValue();

      Map<String, InstanceEbsVolumes.Status> volumes = Maps.newHashMap();

      // Tag these volumes on creation
      List<Tag> tags = ec2TagHelper.getInstanceTags(template, virtualInstanceId, userDefinedTags);
      TagSpecification tagSpecification = new TagSpecification().withTags(tags).withResourceType(ResourceType.Volume);

      for (int j = 0; j < volumesPerInstance; j++) {
        CreateVolumeRequest request = new CreateVolumeRequest()
            .withVolumeType(template.getEbsVolumeType())
            .withSize(template.getEbsVolumeSizeGiB())
            .withAvailabilityZone(availabilityZone)
            .withEncrypted(template.isEnableEbsEncryption())
            .withKmsKeyId(template.getEbsKmsKeyId().get())
            .withTagSpecifications(tagSpecification);

        if (template.getEbsIops().isPresent()) {
          request.withIops(template.getEbsIops().get());
        }

        try {
          CreateVolumeResult result = client.createVolume(request);
          String volumeId = result.getVolume().getVolumeId();
          volumes.put(volumeId, InstanceEbsVolumes.Status.CREATED);
        } catch (AmazonServiceException ex) {
          String message = "Failed to request an EBS volume for virtual instance %s";
          LOG.error(String.format(message, virtualInstanceId), ex);
          String volumeId = InstanceEbsVolumes.UNCREATED_VOLUME_ID + uncreatedVolumeCount;
          volumes.put(volumeId, InstanceEbsVolumes.Status.FAILED);
          uncreatedVolumeCount++;
        }
      }
      InstanceEbsVolumes instanceEbsVolumes = new InstanceEbsVolumes(virtualInstanceId, ec2InstanceId, volumes);
      instanceEbsVolumesList.add(instanceEbsVolumes);
    }
    return instanceEbsVolumesList;
  }

  /**
   * Returns all volumes from a list of instance EBS volumes that have the specified status.
   */
  private static Set<String> getAllVolumeIdsWithStatus(List<InstanceEbsVolumes> instanceEbsVolumes,
      InstanceEbsVolumes.Status status) {
    Set<String> volumeIds = Sets.newHashSet();
    for (InstanceEbsVolumes instance : instanceEbsVolumes) {
      for (Map.Entry<String, InstanceEbsVolumes.Status> volume : instance.getVolumeStatuses().entrySet()) {
        String volumeId = volume.getKey();
        InstanceEbsVolumes.Status volumeStatus = volume.getValue();
        if (volumeStatus == status) volumeIds.add(volumeId);
      }
    }
    return volumeIds;
  }

  /**
   * Returns true if all volumes for an instance has the expected status.
   */
  private static boolean instanceHasAllVolumesWithStatus(InstanceEbsVolumes instanceEbsVolumes,
      InstanceEbsVolumes.Status status) {
    int volumeCount = instanceEbsVolumes.getVolumeStatuses().size();
    Set<String> volumesWithExpectedStatus =
        getAllVolumeIdsWithStatus(Collections.singletonList(instanceEbsVolumes), status);
    return volumeCount == volumesWithExpectedStatus.size();
  }

  /**
   * Waits for the volumes in a list of {@code InstanceEbsVolumes} to reach an available state.
   * Returns an updated list of {@code InstanceEbsVolumes} with the volumes that became
   * available marked as AVAILABLE and volumes that failed or timed out marked as FAILED.
   *
   * @param createdInstanceVolumes list of instances with their created ebs volumes
   * @return updated list of instances EBS volumes
   */
  public List<InstanceEbsVolumes> waitUntilVolumesAvailable(List<InstanceEbsVolumes> createdInstanceVolumes)
      throws InterruptedException {

    Set<String> volumesToCheck = getAllVolumeIdsWithStatus(createdInstanceVolumes, InstanceEbsVolumes.Status.CREATED);

    int numRequestedVolumes = volumesToCheck.size();
    Set<String> volumesAvailable = Sets.newHashSetWithExpectedSize(numRequestedVolumes);

    if (numRequestedVolumes > 0) {
      LOG.info("Waiting for a maximum of {} seconds for volumes to become available",
          availableTimeoutSeconds);

      Stopwatch watch = Stopwatch.createStarted();
      while (watch.elapsed(TimeUnit.SECONDS) < availableTimeoutSeconds) {
        DescribeVolumesRequest volumeRequest = new DescribeVolumesRequest()
            .withVolumeIds(volumesToCheck);

        try {
          List<Volume> volumes = client.describeVolumes(volumeRequest).getVolumes();

          for (Volume volume : volumes) {
            String id = volume.getVolumeId();
            VolumeState state = VolumeState.fromValue(volume.getState());

            switch (state) {
              case Creating:
                break;
              case Available:
                volumesToCheck.remove(id);
                volumesAvailable.add(id);
                break;
              case Error:
                // TODO log why the volume failed which may need a separate api call
                volumesToCheck.remove(id);
                break;
              default:
                String err = String.format("A requested volume went into an unexpected state %s while waiting " +
                    "for volume to become available", state);
                throw new IllegalStateException(String.format(err, state));
            }
          }

          if (volumesToCheck.isEmpty()) {
            break;
          }
        } catch (AmazonServiceException ex) {
          // ignore exception when volume isn't found, newly created volumes may not be found right away
          if (ex.getErrorCode().equals("InvalidVolume.NotFound")) {
            LOG.info("Requested volume(s) not yet found");
          } else {
            throw AWSExceptions.propagate(ex);
          }
        }

        LOG.info("Waiting on {} out of {} volumes to reach a final state, next check in {} seconds",
            volumesToCheck.size(), numRequestedVolumes, WAIT_UNTIL_AVAILABLE_INTERVAL_SECONDS);
        TimeUnit.SECONDS.sleep(WAIT_UNTIL_AVAILABLE_INTERVAL_SECONDS);
      }

      if (volumesToCheck.size() > 0) {
        LOG.error("Timed out while waiting for volumes to be created, {} out of {} volumes became available",
            volumesAvailable.size(), numRequestedVolumes);
      }
    } else {
      LOG.info("Skipping wait for availability because no EBS volumes were created");
    }

    // Update the status of each volume to AVAILABLE or FAILED based on the result

    List<InstanceEbsVolumes> updated = Lists.newArrayList();
    for (InstanceEbsVolumes instanceEbsVolumes : createdInstanceVolumes) {
      Map<String, InstanceEbsVolumes.Status> updatedVolumes = Maps.newHashMap();
      for (String volumeId : instanceEbsVolumes.getVolumeStatuses().keySet()) {
        InstanceEbsVolumes.Status updatedStatus = volumesAvailable.contains(volumeId) ?
            InstanceEbsVolumes.Status.AVAILABLE :
            InstanceEbsVolumes.Status.FAILED;
        updatedVolumes.put(volumeId, updatedStatus);
      }
      updated.add(new InstanceEbsVolumes(instanceEbsVolumes.getVirtualInstanceId(),
          instanceEbsVolumes.getEc2InstanceId(), updatedVolumes));
    }

    return updated;
  }

  /**
   * Goes through a list of {@code InstanceEbsVolumes} and attaches all AVAILABLE
   * volumes to its associated instance. If an instance has any volume that isn't
   * in the AVAILABLE stage, all volumes for that instance will skip tagging and
   * attachment steps.
   *
   * @param template               the instance template
   * @param instanceEbsVolumesList a list of instances with their associated volumes
   * @throws InterruptedException if the operation is interrupted
   */
  public List<InstanceEbsVolumes> attachVolumes(EC2InstanceTemplate template,
      List<InstanceEbsVolumes> instanceEbsVolumesList) throws InterruptedException {

    Set<String> requestedAttachments = Sets.newHashSet();
    DateTime timeout = DateTime.now().plus(availableTimeoutSeconds);

    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      String ec2InstanceId = instanceEbsVolumes.getEc2InstanceId();

      if (!instanceHasAllVolumesWithStatus(instanceEbsVolumes, InstanceEbsVolumes.Status.AVAILABLE)) {
        continue;
      }

      Map<String, InstanceEbsVolumes.Status> volumes = instanceEbsVolumes.getVolumeStatuses();
      List<String> deviceNames = ebsDeviceMappings.getDeviceNames(volumes.size());

      int index = 0;
      for (String volumeId : instanceEbsVolumes.getVolumeStatuses().keySet()) {
        String deviceName = deviceNames.get(index);
        final AttachVolumeRequest volumeRequest = new AttachVolumeRequest()
            .withVolumeId(volumeId)
            .withInstanceId(ec2InstanceId)
            .withDevice(deviceName);
        index++;

        LOG.info(">> Attaching volume {} to instance {} with device name {}", volumeId, ec2InstanceId, deviceName);

        try {
          retryUntil(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  client.attachVolume(volumeRequest);
                  return null;
                }
              },
              timeout);
          requestedAttachments.add(volumeId);
        } catch (RetryException e) {
          LOG.warn("timeout attaching volume {} to instance {}", volumeId, ec2InstanceId);
        } catch (ExecutionException ex) {
          if (AmazonServiceException.class.isInstance(ex.getCause())) {
            AWSExceptions.propagateIfUnrecoverable((AmazonServiceException) ex.getCause());
          }

          LOG.error(String.format("Failed to attach volume %s to instance %s with device name %s",
              volumeId, ec2InstanceId, deviceName), ex.getCause());
        }
      }
    }

    Collection<String> attachedVolumes = waitUntilVolumesAttached(requestedAttachments);

    // Update the status of each volume to ATTACHED or FAILED based on the result

    List<InstanceEbsVolumes> updated = Lists.newArrayList();
    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      Map<String, InstanceEbsVolumes.Status> updatedVolumes = Maps.newHashMap();
      for (String volumeId : instanceEbsVolumes.getVolumeStatuses().keySet()) {
        InstanceEbsVolumes.Status updatedStatus = attachedVolumes.contains(volumeId) ?
            InstanceEbsVolumes.Status.ATTACHED :
            InstanceEbsVolumes.Status.FAILED;
        updatedVolumes.put(volumeId, updatedStatus);
      }
      updated.add(new InstanceEbsVolumes(instanceEbsVolumes.getVirtualInstanceId(),
          instanceEbsVolumes.getEc2InstanceId(), updatedVolumes));
    }

    return updated;
  }

  /**
   * Deletes a specified collection of volumes.
   *
   * @param volumeIds the collection of volume ids to delete.
   */
  public void deleteVolumes(Collection<String> volumeIds) {
    LOG.info(">> Deleting {} volumes", volumeIds.size());
    AmazonClientException ex = null;

    for (String id : volumeIds) {
      // TODO volumes that are attached need to be detached before we can delete
      DeleteVolumeRequest request = new DeleteVolumeRequest().withVolumeId(id);
      try {
        client.deleteVolume(request);
      } catch (AmazonClientException e) {
        LOG.error("<< Failed to delete volume " + id, e);
        ex = (ex == null ? e : ex);
      }
    }

    if (ex != null) {
      AWSExceptions.propagate(ex);
    }
  }

  /**
   * Adds a delete on termination flag to all volumes in an {@code InstanceEbsVolumes} list
   * that have the ATTACHED status. This makes sure that the volumes associated with the
   * instance will be automatically cleaned up upon instance termination.
   *
   * @param instanceEbsVolumesList list of instances along with their associated volumes
   */
  public void addDeleteOnTerminationFlag(List<InstanceEbsVolumes> instanceEbsVolumesList) throws Exception {
    final Set<String> volumesToFlag = getAllVolumeIdsWithStatus(
        instanceEbsVolumesList, InstanceEbsVolumes.Status.ATTACHED);

    if (!volumesToFlag.isEmpty()) {
      DateTime timeout = DateTime.now().plus(availableTimeoutSeconds);
      for (final InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
        Callable<Void> task = new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            String ec2InstanceId = instanceEbsVolumes.getEc2InstanceId();

            DescribeInstanceAttributeRequest instanceAttributeRequest = new DescribeInstanceAttributeRequest()
                .withAttribute(InstanceAttributeName.BlockDeviceMapping)
                .withInstanceId(ec2InstanceId);

            List<InstanceBlockDeviceMapping> blockDeviceMappings =
                client.describeInstanceAttribute(instanceAttributeRequest)
                    .getInstanceAttribute()
                    .getBlockDeviceMappings();

            for (InstanceBlockDeviceMapping blockDeviceMapping : blockDeviceMappings) {
              String volumeId = blockDeviceMapping.getEbs().getVolumeId();

              // The block device mapping may have volumes associated with it that were not
              // provisioned by us. We skip marking those volumes for deletion.

              if (!volumesToFlag.contains(volumeId)) {
                continue;
              }

              InstanceBlockDeviceMappingSpecification updatedSpec = new InstanceBlockDeviceMappingSpecification()
                  .withEbs(
                      new EbsInstanceBlockDeviceSpecification()
                          .withDeleteOnTermination(true)
                          .withVolumeId(volumeId)
                  )
                  .withDeviceName(blockDeviceMapping.getDeviceName());

              ModifyInstanceAttributeRequest modifyRequest = new ModifyInstanceAttributeRequest()
                  .withBlockDeviceMappings(updatedSpec)
                  .withInstanceId(ec2InstanceId);

              client.modifyInstanceAttribute(modifyRequest);
            }

            return null;
          }
        };

        retryUntil(task, timeout);
      }
    }
  }

  /**
   * Wait for the specified list of volumes to be attached within a timeout.
   *
   * @param volumeIds collection of volume ids to wait for
   * @return collection of volumes that were successfully attached
   * @throws InterruptedException if the operation is interrupted
   */
  private Collection<String> waitUntilVolumesAttached(Collection<String> volumeIds) throws InterruptedException {
    checkNotNull(volumeIds);
    if (volumeIds.isEmpty()) {
      LOG.info("No volumes are being attached, skipping wait");
      return Collections.emptySet();
    }

    Set<String> unattachedVolumes = Sets.newHashSet(volumeIds);
    Set<String> attachedVolumes = Sets.newHashSet();

    LOG.info("Waiting for a maximum of {} seconds for volumes to be attached",
        attachTimeoutSeconds);

    Stopwatch watch = Stopwatch.createStarted();
    while (watch.elapsed(TimeUnit.SECONDS) < attachTimeoutSeconds) {
      DescribeVolumesRequest volumeRequest = new DescribeVolumesRequest().withVolumeIds(unattachedVolumes);
      List<Volume> volumes = client.describeVolumes(volumeRequest).getVolumes();

      for (Volume volume : volumes) {
        VolumeAttachment attachment = Iterables.getOnlyElement(volume.getAttachments());
        VolumeAttachmentState state = VolumeAttachmentState.fromValue(attachment.getState());

        if (state == VolumeAttachmentState.Attached) {
          unattachedVolumes.remove(volume.getVolumeId());
          attachedVolumes.add(volume.getVolumeId());
        }
      }

      if (unattachedVolumes.isEmpty()) {
        return attachedVolumes;
      }

      LOG.info("Waiting on {} out of {} volumes to be attached, next check in {} seconds",
          unattachedVolumes.size(), volumeIds.size(), WAIT_UNTIL_ATTACHED_INTERVAL_SECONDS);
      TimeUnit.SECONDS.sleep(WAIT_UNTIL_ATTACHED_INTERVAL_SECONDS);
    }

    LOG.error("Timed out while waiting for all volumes to be attached, {} out of {} volumes were attached",
        attachedVolumes.size(), volumeIds.size());
    return attachedVolumes;
  }

  /**
   * Get the availability zone from a Subnet ID.
   *
   * @param subnetId the id of the subnet
   * @return the availability zone of the subnet
   */
  private String getAvailabilityZoneFromSubnetId(String subnetId) {
    DescribeSubnetsRequest request = new DescribeSubnetsRequest().withSubnetIds(subnetId);
    DescribeSubnetsResult result = client.describeSubnets(request);
    Subnet subnet = Iterables.getOnlyElement(result.getSubnets());
    return subnet.getAvailabilityZone();
  }
}
