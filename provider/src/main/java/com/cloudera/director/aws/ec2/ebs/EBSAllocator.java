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
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.DeleteVolumeRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.DetachVolumeRequest;
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
  /**
   * The key for the timeout to wait for EBS volumes to be detached.
   */
  public static final String TIMEOUT_DETACH = "ec2.ebs.detachSeconds";

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
  private final long detachTimeoutSeconds;
  private final EC2TagHelper ec2TagHelper;
  private final EBSDeviceMappings ebsDeviceMappings;
  private final Set<String> excludeDeviceNames;
  private final boolean useTagOnCreate;

  /**
   * Constructs a new EBS allocator instance.
   *
   * @param client       a pre-configured ec2 client
   * @param awsTimeouts  the AWS timeouts
   * @param ec2TagHelper the custom tag mappings
   * @param ebsDeviceMappings helper object to retrieve device mappings
   * @param excludeDeviceNames set of device names that should be excluded when attaching the volumes
   */
  public EBSAllocator(AmazonEC2Client client, AWSTimeouts awsTimeouts,
      EC2TagHelper ec2TagHelper, EBSDeviceMappings ebsDeviceMappings,
      Set<String> excludeDeviceNames, boolean useTagOnCreate) {
    checkNotNull(awsTimeouts, "awsTimeouts is null");

    this.client = checkNotNull(client, "ec2 client is null");
    this.availableTimeoutSeconds =
        awsTimeouts.getTimeout(TIMEOUT_AVAILABLE).or(DEFAULT_TIMEOUT_SECONDS);
    this.attachTimeoutSeconds =
        awsTimeouts.getTimeout(TIMEOUT_ATTACH).or(DEFAULT_TIMEOUT_SECONDS);
    this.detachTimeoutSeconds =
        awsTimeouts.getTimeout(TIMEOUT_DETACH).or(DEFAULT_TIMEOUT_SECONDS);
    this.ec2TagHelper = checkNotNull(ec2TagHelper, "ec2TagHelper is null");
    this.ebsDeviceMappings = checkNotNull(ebsDeviceMappings, "ebsDeviceMappings is null");
    this.excludeDeviceNames = checkNotNull(excludeDeviceNames, "excludeDeviceNames is null");
    this.useTagOnCreate = useTagOnCreate;
  }

  /**
   * Represents an instance along with the EBS volumes associated with it.
   */
  public static class InstanceEbsVolumes {

    public static final String UNCREATED_VOLUME_ID = "uncreated";

    private final String virtualInstanceId;
    private final String ec2InstanceId;
    private final Map<String, VolumeState> volumeStates;

    /**
     * Constructor.
     *
     * @param virtualInstanceId the Director virtual instance id
     * @param ec2InstanceId     the AWS EC2 instance id
     * @param volumeStates      the volume ids for each instance along with the status of the volume
     */
    public InstanceEbsVolumes(String virtualInstanceId, String ec2InstanceId, Map<String, VolumeState> volumeStates) {
      this.virtualInstanceId = requireNonNull(virtualInstanceId, "virtualInstanceId is null");
      this.ec2InstanceId = requireNonNull(ec2InstanceId, "ec2InstanceId is null");
      this.volumeStates = ImmutableMap.copyOf(volumeStates);
    }

    public String getVirtualInstanceId() {
      return virtualInstanceId;
    }

    public String getEc2InstanceId() {
      return ec2InstanceId;
    }

    public Map<String, VolumeState> getVolumeStates() {
      return volumeStates;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InstanceEbsVolumes that = (InstanceEbsVolumes) o;

      if (!virtualInstanceId.equals(that.virtualInstanceId)) return false;
      if (!ec2InstanceId.equals(that.ec2InstanceId)) return false;
      return volumeStates.equals(that.volumeStates);

    }

    @Override
    public int hashCode() {
      int result = virtualInstanceId.hashCode();
      result = 31 * result + ec2InstanceId.hashCode();
      result = 31 * result + volumeStates.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "InstanceEbsVolumes{" +
          "virtualInstanceId='" + virtualInstanceId + '\'' +
          ", ec2InstanceId='" + ec2InstanceId + '\'' +
          ", volumeStates=" + volumeStates +
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

      Map<String, VolumeState> volumes = Maps.newHashMap();

      // Tag these volumes on creation
      List<Tag> tags = ec2TagHelper.getInstanceTags(template, virtualInstanceId, userDefinedTags);
      TagSpecification tagSpecification = new TagSpecification().withTags(tags).withResourceType(ResourceType.Volume);

      List<CreateVolumeRequest> createVolumeRequests = Lists.newArrayList();

      for (SystemDisk systemDisk : template.getSystemDisks()) {
        CreateVolumeRequest createVolumeRequest =
            systemDisk.toCreateVolumeRequest(availabilityZone, tagSpecification);
        createVolumeRequests.add(createVolumeRequest);
      }

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
        createVolumeRequests.add(request);
      }

      for (CreateVolumeRequest request : createVolumeRequests) {
        try {
          CreateVolumeResult result = client.createVolume(request);
          String volumeId = result.getVolume().getVolumeId();
          volumes.put(volumeId, VolumeState.Creating);
        } catch (AmazonServiceException ex) {
          String message = "Failed to request an EBS volume for virtual instance %s";
          LOG.error(String.format(message, virtualInstanceId), ex);
          String volumeId = InstanceEbsVolumes.UNCREATED_VOLUME_ID + uncreatedVolumeCount;
          volumes.put(volumeId, VolumeState.Error);
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
  private static Set<String> getAllVolumeIdsWithState(List<InstanceEbsVolumes> instanceEbsVolumes,
                                                      VolumeState desiredState) {
    Set<String> volumeIds = Sets.newHashSet();
    for (InstanceEbsVolumes instance : instanceEbsVolumes) {
      for (Map.Entry<String, VolumeState> volume : instance.getVolumeStates().entrySet()) {
        String volumeId = volume.getKey();
        VolumeState volumeState = volume.getValue();
        if (volumeState == desiredState) volumeIds.add(volumeId);
      }
    }
    return volumeIds;
  }

  /**
   * Returns true if all volumes for an instance has the expected status.
   */
  private static boolean instanceHasAllVolumesWithState(InstanceEbsVolumes instanceEbsVolumes,
                                                        VolumeState expectedState) {
    int volumeCount = instanceEbsVolumes.getVolumeStates().size();
    Set<String> volumesWithExpectedState =
        getAllVolumeIdsWithState(Collections.singletonList(instanceEbsVolumes), expectedState);
    return volumeCount == volumesWithExpectedState.size();
  }

  /**
   * Waits for the volumes in a list of {@code InstanceEbsVolumes} to reach an available state.
   * Returns an updated list of {@code InstanceEbsVolumes} with the volumes that became
   * available marked as Available and volumes that failed or timed out marked as Error.
   *
   * @param createdInstanceVolumes list of instances with their created ebs volumes
   * @return updated list of instances EBS volumes
   */
  public List<InstanceEbsVolumes> waitUntilVolumesAvailable(List<InstanceEbsVolumes> createdInstanceVolumes)
      throws InterruptedException {

    Set<String> volumesToCheck = getAllVolumeIdsWithState(createdInstanceVolumes, VolumeState.Creating);

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

    // Update the status of each volume to Available or Error based on the result

    List<InstanceEbsVolumes> updated = Lists.newArrayList();
    for (InstanceEbsVolumes instanceEbsVolumes : createdInstanceVolumes) {
      Map<String, VolumeState> updatedVolumes = Maps.newHashMap();
      for (String volumeId : instanceEbsVolumes.getVolumeStates().keySet()) {
        VolumeState updatedState = volumesAvailable.contains(volumeId) ?
            VolumeState.Available : VolumeState.Error;
        updatedVolumes.put(volumeId, updatedState);
      }
      updated.add(new InstanceEbsVolumes(instanceEbsVolumes.getVirtualInstanceId(),
          instanceEbsVolumes.getEc2InstanceId(), updatedVolumes));
    }

    return updated;
  }

  /**
   * Goes through a list of {@code InstanceEbsVolumes} and attaches all Available
   * volumes to its associated instance. If an instance has any volume that isn't
   * in the Available state, all volumes for that instance will skip tagging and
   * attachment steps. If useTagOnCreate is false, the volumes will be tagged as well.
   *
   * @param template               the instance template
   * @param instanceEbsVolumesList a list of instances with their associated volumes
   * @throws InterruptedException if the operation is interrupted
   */
  public List<InstanceEbsVolumes> attachAndOptionallyTagVolumes(EC2InstanceTemplate template,
      List<InstanceEbsVolumes> instanceEbsVolumesList) throws InterruptedException {

    Set<String> requestedAttachments = Sets.newHashSet();
    DateTime timeout = DateTime.now().plusSeconds((int) availableTimeoutSeconds);

    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      String virtualInstanceId = instanceEbsVolumes.getVirtualInstanceId();
      String ec2InstanceId = instanceEbsVolumes.getEc2InstanceId();

      if (!instanceHasAllVolumesWithState(instanceEbsVolumes, VolumeState.Available)) {
        continue;
      }

      // Pre-compute user-defined tags for efficiency
      List<Tag> userDefinedTags = ec2TagHelper.getUserDefinedTags(template);

      Map<String, VolumeState> volumes = instanceEbsVolumes.getVolumeStates();
      List<String> deviceNames = ebsDeviceMappings.getDeviceNames(volumes.size(), excludeDeviceNames);

      int index = 0;
      for (String volumeId : instanceEbsVolumes.getVolumeStates().keySet()) {
        if (!useTagOnCreate) {
          tagVolume(template, userDefinedTags, virtualInstanceId, volumeId);
        }

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

    // Update the status of each volume to InUse or Error based on the result

    List<InstanceEbsVolumes> updated = Lists.newArrayList();
    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      Map<String, VolumeState> updatedVolumes = Maps.newHashMap();
      for (String volumeId : instanceEbsVolumes.getVolumeStates().keySet()) {
        VolumeState updatedState = attachedVolumes.contains(volumeId) ?
            VolumeState.InUse : VolumeState.Error;
        updatedVolumes.put(volumeId, updatedState);
      }
      updated.add(new InstanceEbsVolumes(instanceEbsVolumes.getVirtualInstanceId(),
          instanceEbsVolumes.getEc2InstanceId(), updatedVolumes));
    }

    return updated;
  }

  /**
   * Deletes a specified collection of volumes.
   *
   * @param volumeIdsAndStates the collection of volume ids to delete along with their current state
   */
  public void deleteVolumes(Map<String, VolumeState> volumeIdsAndStates) throws InterruptedException {
    LOG.info(">> Deleting {} volumes", volumeIdsAndStates.size());
    AmazonClientException ex = null;

    for (Map.Entry<String, VolumeState> idAndState : volumeIdsAndStates.entrySet()) {
      String id = idAndState.getKey();
      VolumeState volumeState = idAndState.getValue();

      if (volumeState == VolumeState.InUse) {
        boolean detached = false;

        try {
          LOG.info("Detaching volume {}.", id);
          DetachVolumeRequest detachVolumeRequest = new DetachVolumeRequest()
              .withVolumeId(id);
          client.detachVolume(detachVolumeRequest);

          // Wait for the instance to detach
          DateTime timeout = DateTime.now().plusSeconds((int) detachTimeoutSeconds);
          while (DateTime.now().isBefore(timeout)) {
            DescribeVolumesResult describeVolumeResult = client.describeVolumes(new DescribeVolumesRequest()
                .withVolumeIds(id));

            if (describeVolumeResult.getVolumes().size() != 1) {
              continue;
            }

            Volume volume = describeVolumeResult.getVolumes().get(0);

            boolean allDetached = true;
            for (VolumeAttachment attachment : volume.getAttachments()) {
              if (VolumeAttachmentState.fromValue(attachment.getState()) != VolumeAttachmentState.Detached) {
                allDetached = false;
                break;
              }
            }

            if (allDetached) {
              LOG.info("Volume {} successfully detached", id);
              detached = true;
              break;
            }

            LOG.info("Waiting for volume {} to detach, next check in 5 seconds.", id);
            TimeUnit.SECONDS.sleep(5);
          }

        } catch (AmazonClientException e) {
          if (!AWSExceptions.isNotFound(e)) {
            LOG.error("<< Failed to detach volume " + id, e);
          } else {
            LOG.warn("Unable to find {}, proceeding to next volume.", id);
            continue;
          }
        }

        if (!detached) {
          LOG.warn("Unable to detach {}, proceeding to next volume.", id);
          continue;
        }
      }

      DeleteVolumeRequest request = new DeleteVolumeRequest().withVolumeId(id);

      try {
        client.deleteVolume(request);
        LOG.info("Volume {} deleted.", id);
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
   * that are attached to a Director managed instance. This makes sure that the volumes associated with the
   * instance will be automatically cleaned up upon instance termination.
   *
   * @param instanceEbsVolumesList list of instances along with their associated volumes
   */
  public void addDeleteOnTerminationFlag(List<InstanceEbsVolumes> instanceEbsVolumesList) throws Exception {
    DateTime timeout = DateTime.now().plusSeconds((int) availableTimeoutSeconds);
    for (final InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      Callable<Void> task = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          String ec2InstanceId = instanceEbsVolumes.getEc2InstanceId();
          Set<String> managedVolumes = instanceEbsVolumes.getVolumeStates().keySet();

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

            if (!managedVolumes.contains(volumeId)) {
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

            try {
              client.modifyInstanceAttribute(modifyRequest);
            } catch (AmazonClientException e) {
              LOG.error("Volume {} failed to attach.", volumeId);
            }
          }

          return null;
        }
      };

      retryUntil(task, timeout);
    }
  }

  /**
   * Returns the updated volume info for the given volumes.
   *
   * @param instanceEbsVolumesList the list of instance EBS volumes to query
   * @return an updated list of instance EBS volumes
   */
  public List<InstanceEbsVolumes> getUpdatedVolumeInfo(List<InstanceEbsVolumes> instanceEbsVolumesList) {
    List<InstanceEbsVolumes> updatedInstanceEbsVolumesList =
        Lists.newArrayListWithExpectedSize(instanceEbsVolumesList.size());

    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      Map<String, VolumeState> volumeStatuses = Maps.newHashMap();
      Set<String> volumeIds = instanceEbsVolumes.getVolumeStates().keySet();
      DescribeVolumesRequest describeVolumesRequest = new DescribeVolumesRequest()
          .withVolumeIds(volumeIds);

      for (Volume volume : client.describeVolumes(describeVolumesRequest).getVolumes()) {
        volumeStatuses.put(volume.getVolumeId(), VolumeState.fromValue(volume.getState()));
      }

      // If we've lost any volume IDs, add them in but label them as Error.
      Set<String> missingVolumeIds = Sets.difference(volumeIds, volumeStatuses.keySet());
      if (!missingVolumeIds.isEmpty()) {
        for (String volumeId : missingVolumeIds) {
          volumeStatuses.put(volumeId, VolumeState.Error);
        }
      }

      updatedInstanceEbsVolumesList.add(new InstanceEbsVolumes(instanceEbsVolumes.getVirtualInstanceId(),
          instanceEbsVolumes.getEc2InstanceId(), volumeStatuses));
    }

    return updatedInstanceEbsVolumesList;
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
   * Tags an EBS volume. Expects that the volume already exists or is in the process of
   * being created.
   *
   * @param template          the instance template
   * @param userDefinedTags   the user-defined tags
   * @param virtualInstanceId the virtual instance id of it's associated instance
   * @param volumeId          the volume id
   * @throws InterruptedException if the operation is interrupted
   */
  private void tagVolume(EC2InstanceTemplate template, List<Tag> userDefinedTags,
      String virtualInstanceId, String volumeId) throws InterruptedException {
    LOG.info(">> Tagging volume {} / {}", volumeId, virtualInstanceId);
    List<Tag> tags = ec2TagHelper.getInstanceTags(template, virtualInstanceId, userDefinedTags);
    client.createTags(new CreateTagsRequest().withTags(tags).withResources(volumeId));
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
