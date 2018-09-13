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

package com.cloudera.director.aws.ec2.allocation.ondemand;

import static com.cloudera.director.aws.AWSExceptions.INSTANCE_LIMIT_EXCEEDED;
import static com.cloudera.director.aws.AWSExceptions.INSUFFICIENT_INSTANCE_CAPACITY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StateReason;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.allocation.AbstractInstanceAllocator;
import com.cloudera.director.aws.ec2.allocation.AllocationHelper;
import com.cloudera.director.spi.v2.model.exception.TransientProviderException;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Holds state and logic for allocating a group of on-demand instances. A new instance is
 * required for each allocation request.</p>
 * <p>The {@link #allocate()} method atomically allocates multiple EC2 instances with the
 * specified identifiers based on a single configured instance template.</p>
 */
@SuppressWarnings({"Guava", "PMD.TooManyStaticImports"})
public class OnDemandAllocator extends AbstractInstanceAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(OnDemandAllocator.class);

  private static final Function<Instance, String> INSTANCE_TO_INSTANCE_ID =
      instance -> {
        requireNonNull(instance, "instance is null");
        return instance.getInstanceId();
      };

  private final boolean useTagOnCreate;

  public OnDemandAllocator(AllocationHelper allocationHelper,
      AmazonEC2AsyncClient client, boolean tagEbsVolumes, boolean useTagOnCreate,
      EC2InstanceTemplate template, Collection<String> virtualInstanceIds, int minCount) {
    super(allocationHelper, client, tagEbsVolumes, template, virtualInstanceIds, minCount);
    this.useTagOnCreate = useTagOnCreate;
  }

  @Override
  public Collection<EC2Instance> allocate() throws InterruptedException {
    return allocateOnDemandInstances(template, virtualInstanceIds, minCount);
  }

  @Override
  public void delete() throws InterruptedException {
    allocationHelper.delete(template, virtualInstanceIds);
  }

  /**
   * Atomically allocates multiple regular EC2 instances with the specified identifiers based on a
   * single instance template. If not all the instances can be allocated, the number of instances
   * allocated must be at least the specified minimum or the method must fail cleanly with no
   * billing implications.
   *
   * @param template           the instance template
   * @param virtualInstanceIds the unique identifiers for the instances
   * @param minCount           the minimum number of instances to allocate if not all resources can be allocated
   * @return the virtual instance ids of the instances that were allocated
   * @throws InterruptedException if the operation is interrupted
   */
  private Collection<EC2Instance> allocateOnDemandInstances(
      EC2InstanceTemplate template, Collection<String> virtualInstanceIds, int minCount)
      throws InterruptedException {

    int instanceCount = virtualInstanceIds.size();

    LOG.info(">> Requesting {} instances for {}", instanceCount, template);

    boolean success = false;
    Map<String, Instance> virtualInstanceIdToInstances = Maps.newHashMapWithExpectedSize(virtualInstanceIds.size());
    Map<String, Instance> unsuccessfulInstances = Maps.newHashMap();

    try {
      // Try to find all instances that are not in a terminal state
      Iterable<Map.Entry<String, Instance>> vIdToInstances = allocationHelper.doFind(
          template, virtualInstanceIds,
          Predicates.not(allocationHelper.INSTANCE_IS_TERMINAL));

      for (Map.Entry<String, Instance> virtualInstanceIdToInstance : vIdToInstances) {
        virtualInstanceIdToInstances.put(
            virtualInstanceIdToInstance.getKey(),
            virtualInstanceIdToInstance.getValue());
      }

      if (!virtualInstanceIdToInstances.isEmpty()) {
        LOG.info("Instances with the following virtual instance IDs were already found allocated: {}",
            virtualInstanceIdToInstances.keySet());
      }

      List<Tag> userDefinedTags = ec2TagHelper.getUserDefinedTags(template);
      List<RunInstancesResult> runInstancesResults = Lists.newArrayList();
      Set<String> unallocatedInstanceIds = Sets.difference(
          Sets.newHashSet(virtualInstanceIds), virtualInstanceIdToInstances.keySet());

      LOG.info(">> Building {} instance requests", unallocatedInstanceIds.size());

      Set<Exception> encounteredExceptions = Sets.newHashSet();

      if (useTagOnCreate) {
        Map<String, Future<RunInstancesResult>> runInstanceRequests = Maps.newHashMap();
        for (String virtualInstanceId : unallocatedInstanceIds) {
          runInstanceRequests.put(virtualInstanceId, client.runInstancesAsync(
              newRunInstancesRequest(template, virtualInstanceId, userDefinedTags)));
        }

        LOG.info(">> Submitted {} run instance requests.", runInstanceRequests.size());

        // Map of encountered AWS exceptions where key is the AWS error code, which we may propagate
        // later. It should be sufficient to just keep track of one exception per error code.

        for (Map.Entry<String, Future<RunInstancesResult>> runInstanceRequest : runInstanceRequests.entrySet()) {
          String virtualInstanceId = runInstanceRequest.getKey();
          try {
            RunInstancesResult result = runInstanceRequest.getValue().get();
            runInstancesResults.add(result);
            virtualInstanceIdToInstances.put(
                virtualInstanceId,
                getOnlyElement(result.getReservation().getInstances()));

          } catch (ExecutionException e) {
            if (e.getCause() instanceof AmazonServiceException) {
              AmazonServiceException awsException = (AmazonServiceException) e.getCause();
              LOG.error("AWS error while requesting instance {}, AWS error code: {}",
                  virtualInstanceId, awsException.getErrorCode());
              encounteredExceptions.add(awsException);
            } else {
              LOG.error("Error while requesting instance {}. Attempting to proceed.", virtualInstanceId);
              encounteredExceptions.add(e);
            }

            LOG.debug("Exception caught:", e);
          }
        }

        if (LOG.isInfoEnabled()) {
          for (RunInstancesResult runInstancesResult : runInstancesResults) {
            LOG.info("<< Reservation {} with {}", runInstancesResult.getReservation().getReservationId(),
                summarizeReservationForLogging(runInstancesResult.getReservation()));
          }
        }
      } else {
        LOG.info("Tag on create is disabled.");

        RunInstancesResult runInstancesResult = null;
        int normalizedMinCount = Math.max(1, minCount - virtualInstanceIdToInstances.size());
        try {
          // Only allocated what we haven't allocated yet
          runInstancesResult = client.runInstances(
              newRunInstanceRequestBulkNoTagOnCreate(template, virtualInstanceIds, normalizedMinCount));
        } catch (AmazonServiceException e) {
          AWSExceptions.propagateIfUnrecoverable(e);

          // As documented at http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-capacity.html

          if (INSUFFICIENT_INSTANCE_CAPACITY.equals(e.getErrorCode()) ||
              INSTANCE_LIMIT_EXCEEDED.equals(e.getErrorCode())) {
            LOG.warn("Hit instance capacity issues. Attempting to proceed anyway.", e);
          } else {
            throw AWSExceptions.propagate(e);
          }
        }

        List<Instance> instances = runInstancesResult != null ? runInstancesResult.getReservation().getInstances()
            : Lists.newArrayList();

        // Limit the number of virtual instance id's used for tagging to the
        // number of instances that we managed to reserve.
        List<String> virtualInstanceIdsAllocated = FluentIterable
            .from(virtualInstanceIds)
            .limit(instances.size())
            .toList();


        for (Map.Entry<String, Instance> entry : zipWith(virtualInstanceIdsAllocated, instances)) {

          String virtualInstanceId = entry.getKey();
          Instance instance = entry.getValue();
          String ec2InstanceId = instance.getInstanceId();

          if (tagInstance(template, userDefinedTags, virtualInstanceId, ec2InstanceId,
              DateTime.now().plus(waitUntilFindableMillis))) {
            virtualInstanceIdToInstances.put(virtualInstanceId,
                instance.withTags(ec2TagHelper.getInstanceTags(template, virtualInstanceId, userDefinedTags)));
          } else {
            unsuccessfulInstances.put(virtualInstanceId, instance);
            LOG.info("<< Instance {} could not be tagged.", ec2InstanceId);
          }
        }
      }

      // Determine which do not yet have a private IP address.

      int numInstancesAlive = virtualInstanceIdToInstances.size();

      if (numInstancesAlive >= minCount) {

        Map<String, Instance> successfulEC2Instances = Maps.newHashMapWithExpectedSize(virtualInstanceIds.size());
        Map<String, String> ec2InstancesWithNoPrivateIp = Maps.newHashMap();
        DateTime timeout = DateTime.now().plus(waitUntilStartedMillis);

        for (Map.Entry<String, Instance> vIdToInstance : virtualInstanceIdToInstances.entrySet()) {
          if (allocationHelper.waitUntilInstanceHasStarted(vIdToInstance.getValue().getInstanceId(), timeout)) {
            if (vIdToInstance.getValue().getPrivateIpAddress() == null) {
              ec2InstancesWithNoPrivateIp.put(vIdToInstance.getKey(), vIdToInstance.getValue().getInstanceId());
            } else {
              successfulEC2Instances.put(vIdToInstance.getKey(), vIdToInstance.getValue());
              LOG.info("<< Instance {} got IP {}",
                  vIdToInstance.getValue().getInstanceId(),
                  vIdToInstance.getValue().getPrivateIpAddress());
            }
          } else {
            LOG.info("<< Instance {} did not start.", vIdToInstance.getValue().getInstanceId());
          }
        }

        // Wait until all of them have a private IP (it should be pretty fast)
        successfulEC2Instances.putAll(waitForPrivateIpAddresses(ec2InstancesWithNoPrivateIp));

        for (Map.Entry<String, Instance> entry : virtualInstanceIdToInstances.entrySet()) {
          String vid = entry.getKey();
          Instance instance = entry.getValue();
          if (!successfulEC2Instances.containsKey(vid)) {
            unsuccessfulInstances.put(vid, instance);
          }
        }

        numInstancesAlive = successfulEC2Instances.size();
        if (numInstancesAlive >= minCount) {

          success = true;

          List<EC2Instance> result = Lists.newArrayListWithCapacity(successfulEC2Instances.size());
          for (Map.Entry<String, Instance> instance : successfulEC2Instances.entrySet()) {
            String virtualInstanceId = allocationHelper.getVirtualInstanceId(instance.getValue().getTags(), "instance");
            if (Objects.equals(instance.getKey(), virtualInstanceId)) {
              result.add(allocationHelper.createInstance(template, virtualInstanceId, instance.getValue()));
            } else {
              LOG.error("Unable to find corresponding instance for ID {}.", virtualInstanceId);
            }
          }

          return result;
        }
      }

      Set<StateReason> failedStateReasons = getStateReasons(unsuccessfulInstances.values());

      AWSExceptions.propagate("Problem allocating on-demand instances",
          encounteredExceptions, failedStateReasons, template);

      return Collections.emptySet();
    } catch (InterruptedException | UnrecoverableProviderException | TransientProviderException e) {
      throw e;
    } catch (Exception e) {
      throw new UnrecoverableProviderException("Unexpected problem during instance allocation", e);
    } finally {
      if (!success) {
        LOG.error("Unsuccessful allocation of on demand instances. Terminating instances.");

        try {
          Collection<String> ec2InstanceIds = FluentIterable
              .from(virtualInstanceIdToInstances.values())
              .transform(INSTANCE_TO_INSTANCE_ID)
              .toList();
          allocationHelper.doDelete(ec2InstanceIds);
        } catch (InterruptedException e) {
          //noinspection ThrowFromFinallyBlock
          throw e;
        } catch (Exception e) {
          LOG.error("Error while trying to delete instances after failed instance allocation.", e);
        }
      }
    }
  }


  /**
   * Builds a {@code RunInstancesRequest} starting from a template and a virtual instance ID.
   * Instances will be tagged as they're created.
   *
   * @param template          the instance template
   * @param virtualInstanceId the virtual instance ID
   * @param userDefinedTags   user defined tags to attach to the instance
   * @return a RunInstancesRequest object
   */
  @VisibleForTesting
  @SuppressWarnings("ConstantConditions")
  private RunInstancesRequest newRunInstancesRequest(
      EC2InstanceTemplate template, String virtualInstanceId, List<Tag> userDefinedTags) {

    List<Tag> tags = ec2TagHelper.getInstanceTags(template, virtualInstanceId, userDefinedTags);
    List<TagSpecification> tagSpecifications = Lists.newArrayList(
        new TagSpecification().withTags(tags).withResourceType(ResourceType.Instance),
        new TagSpecification().withTags(tags).withResourceType(ResourceType.Volume));

    return newRunInstanceBaseRequest(template)
        .withMinCount(1)
        .withMaxCount(1)
        .withTagSpecifications(tagSpecifications);
  }

  /**
   * Builds a {@code RunInstancesRequest} starting from a template and a collection of virtual instance
   * IDs. Instances will need to be tagged after they're created.
   *
   * @param template           the instance template
   * @param virtualInstanceIds the virtual instance IDs
   * @param minCount           the minimum number of instances to allocate
   * @return a RunInstancesRequest object
   */
  private RunInstancesRequest newRunInstanceRequestBulkNoTagOnCreate(EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds, int minCount) {
    return newRunInstanceBaseRequest(template)
        .withMaxCount(virtualInstanceIds.size())
        .withMinCount(minCount);
  }

  /**
   * Builds a base {@code RunInstancesRequest} object for other run instance request creation objects to build from.
   *
   * @param template the instance template
   * @return a RunInstancesRequest object
   */
  private RunInstancesRequest newRunInstanceBaseRequest(EC2InstanceTemplate template) {
    String image = template.getImage();
    String type = template.getType();

    InstanceNetworkInterfaceSpecification network =
        allocationHelper.getInstanceNetworkInterfaceSpecification(template);

    List<BlockDeviceMapping> deviceMappings = allocationHelper.getBlockDeviceMappings(template);

    LOG.info(">> Instance request type: {}, image: {}", type, image);

    RunInstancesRequest request = new RunInstancesRequest()
        .withImageId(image)
        .withInstanceType(type)
        .withClientToken(UUID.randomUUID().toString())
        .withNetworkInterfaces(network)
        .withBlockDeviceMappings(deviceMappings)
        .withEbsOptimized(template.isEbsOptimized());

    if (template.getIamProfileName().isPresent()) {
      request.withIamInstanceProfile(new IamInstanceProfileSpecification()
          .withName(template.getIamProfileName().get()));
    }

    if (template.getKeyName().isPresent()) {
      request.withKeyName(template.getKeyName().get());
    }

    Placement placement = null;
    if (template.getAvailabilityZone().isPresent()) {
      placement = new Placement().withAvailabilityZone(template.getAvailabilityZone().get());
    }
    if (template.getPlacementGroup().isPresent()) {
      placement = (placement == null) ?
          new Placement().withGroupName(template.getPlacementGroup().get())
          : placement.withGroupName(template.getPlacementGroup().get());
    }
    placement = (placement == null) ?
        new Placement().withTenancy(template.getTenancy())
        : placement.withTenancy(template.getTenancy());

    request.withPlacement(placement);

    Optional<String> userData = template.getUserData();
    if (userData.isPresent()) {
      request.withUserData(userData.get());
    }

    return request;
  }

  /**
   * Takes a collection of instances and extracts termination state reason information. This
   * information will be added to the specified list of plugin exception conditions.
   *
   * @param unsuccessfulInstances the instances
   * @return the termination state reason information
   */
  private Set<StateReason> getStateReasons(Collection<Instance> unsuccessfulInstances) {
    if (unsuccessfulInstances.size() == 0) {
      return Collections.emptySet();
    }

    Set<String> ec2InstanceIds = Sets.newHashSet();
    for (Instance instance : unsuccessfulInstances) {
      ec2InstanceIds.add(instance.getInstanceId());
    }

    DescribeInstancesRequest request = new DescribeInstancesRequest()
        .withInstanceIds(ec2InstanceIds);
    DescribeInstancesResult result = client.describeInstances(request);

    List<Reservation> reservations = result.getReservations();

    // Store the state reason error codes in a map to avoid adding duplicate
    // errors in the accumulator.
    Map<String, StateReason> stateReasons = Maps.newHashMap();

    for (Reservation reservation : reservations) {
      Instance instance = getOnlyElement(reservation.getInstances());
      InstanceStateName stateName = InstanceStateName.fromValue(instance.getState().getName());
      if (stateName != InstanceStateName.Terminated &&
          stateName != InstanceStateName.ShuttingDown) {
        continue;
      }

      StateReason stateReason = instance.getStateReason();
      if (stateReason == null || stateReason.getCode() == null) {
        LOG.error("Instance {} terminated for unknown reason", instance.getInstanceId());
        continue;
      }

      String code = stateReason.getCode();
      String message = stateReason.getMessage();
      LOG.error("Instance {} termination reason: {} (code {})", instance.getInstanceId(), message, code);
      stateReasons.put(code, stateReason);
    }

    return Sets.newHashSet(stateReasons.values());
  }

  /**
   * Returns a summary of the specified reservation suitable for logging.
   *
   * @param reservation the reservation
   * @return a summary of the specified reservation suitable for logging
   */
  private String summarizeReservationForLogging(Reservation reservation) {
    StringBuilder builder = new StringBuilder();
    for (Instance instance : reservation.getInstances()) {
      builder.append(String.format("Instance{id=%s privateIp=%s} ",
          instance.getInstanceId(), instance.getPrivateIpAddress()));
    }
    return builder.toString();
  }

  /**
   * <p>Zip two collections as a lazy iterable of pairs.</p>
   * <p><em>Note:</em> the returned iterable is not suitable for repeated use, since it
   * exhausts the iterator over the first collection.</p>
   *
   * @param a the first collection
   * @param b the second collection
   * @throws IllegalArgumentException if input collections don't have the same size
   */
  @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
  private <K, V> Iterable<Map.Entry<K, V>> zipWith(Collection<K> a, Collection<V> b) {
    checkArgument(a.size() == b.size(), "collections don't have the same size");

    final Iterator<K> iterator = a.iterator();
    return Iterables.transform(b, input -> Maps.immutableEntry(iterator.next(), input));
  }
}
