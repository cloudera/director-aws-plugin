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

import static com.cloudera.director.aws.ec2.EC2Retryer.retryUntil;
import static com.google.common.collect.Iterables.getOnlyElement;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.SpotInstanceState;
import com.amazonaws.services.ec2.model.SpotPlacement;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClient;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.allocation.AbstractInstanceAllocator;
import com.cloudera.director.aws.ec2.allocation.AllocationHelper;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.github.rholder.retry.RetryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Holds state and logic for allocating a group of Spot instances. A new instance is
 * required for each allocation request.</p>
 * <p>The {@link #allocate()} method atomically allocates multiple EC2 Spot Instances with the
 * specified identifiers based on a single configured instance template.</p>
 * <p><em>Note:</em> regarding the good-faith effort in the contract of the SPI not to leak
 * resources, there are some cases where despite failing to satisfy the min count there are billing
 * implications, due to non-atomicity of AWS operations. In particular, if we lose connectivity to
 * AWS for an extended period of time after creating Spot instance requests, we will be unable to
 * cancel the requests, and unable to detect that instances have been provisioned. The resulting
 * requests and/or instances may or may not be tagged appropriately, depending on when connectivity
 * was interrupted.</p>
 */
@SuppressWarnings("Guava")
@VisibleForTesting
public class SpotGroupAllocator extends AbstractInstanceAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(SpotGroupAllocator.class);

  /**
   * Max spot instance count exceeded failure string.
   */
  private static final String MAX_SPOT_INSTANCE_COUNT_EXCEEDED = "MaxSpotInstanceCountExceeded";

  /**
   * The default spot instance request duration, in milliseconds.
   */
  private static final long DEFAULT_SPOT_INSTANCE_REQUEST_DURATION_MS = 10 * 60 * 1000; //10 min

  /**
   * The configuration key for spot instance timeouts.
   */
  private static final String SPOT_INSTANCE_REQUEST_DURATION_MS = "ec2.spot.requestDurationMilliseconds";

  /**
   * The default amount of time to wait, in milliseconds, for a Spot price change when the request Spot
   * price is known to be below the current EC2 Spot price.
   */
  private static final int DEFAULT_SPOT_INSTANCE_PRICE_CHANGE_DURATION_MS = 0;

  /**
   * The latest time to wait for Spot instance request fulfillment.
   */
  private final Date requestExpirationTime;

  /**
   * The latest time to wait for a EC2 Spot price change when it is known that the current EC2 Spot price
   * exceeds the request Spot price.
   */
  private final Date priceChangeDeadlineTime;

  /**
   * The map from virtual instance IDs to the corresponding Spot allocation records.
   */
  private final Map<String, SpotAllocationRecord> spotAllocationRecordsByVirtualInstanceId;

  /**
   * The map from untagged Spot instance request IDs to the corresponding Spot instances.
   */
  private final Map<String, String> spotInstancesByUntaggedSpotInstanceRequestId;

  /**
   * Creates a Spot group allocator with the specified parameters.
   *
   * @param allocationHelper   the allocation helper
   * @param ec2Client          the EC2 client
   * @param stsClient          the STS client
   * @param template           the instance template
   * @param virtualInstanceIds the virtual instance IDs for the created instances
   * @param minCount           the minimum number of instances to allocate if not all resources can be
   *                           allocated
   */
  @VisibleForTesting
  public SpotGroupAllocator(AllocationHelper allocationHelper,
      AmazonEC2AsyncClient ec2Client,
      AWSSecurityTokenServiceAsyncClient stsClient,
      boolean tagEbsVolumes,
      EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds,
      int minCount) {
    super(allocationHelper, ec2Client, stsClient, tagEbsVolumes, template, virtualInstanceIds, minCount);

    AWSTimeouts awsTimeouts = allocationHelper.getAWSTimeouts();

    long startTime = System.currentTimeMillis();
    long spotRequestDurationMillis = awsTimeouts.getTimeout(SPOT_INSTANCE_REQUEST_DURATION_MS)
        .or(DEFAULT_SPOT_INSTANCE_REQUEST_DURATION_MS);
    this.requestExpirationTime = new Date(startTime + spotRequestDurationMillis);
    this.priceChangeDeadlineTime = new Date(startTime + DEFAULT_SPOT_INSTANCE_PRICE_CHANGE_DURATION_MS);

    this.spotAllocationRecordsByVirtualInstanceId =
        initializeSpotAllocationRecordMap(virtualInstanceIds);
    this.spotInstancesByUntaggedSpotInstanceRequestId = Maps.newHashMap();
  }

  /**
   * Initializes the map from virtual instance IDs to the corresponding Spot allocation records.
   *
   * @param virtualInstanceIds the virtual instance IDs
   * @return the map from virtual instance IDs to the corresponding Spot allocation records
   */
  private Map<String, SpotAllocationRecord> initializeSpotAllocationRecordMap(
      Collection<String> virtualInstanceIds) {
    Map<String, SpotAllocationRecord> spotAllocationRecordsByVirtualInstanceId =
        Maps.newLinkedHashMap();
    for (String virtualInstanceId : virtualInstanceIds) {
      SpotAllocationRecord spotAllocationRecord = new SpotAllocationRecord(virtualInstanceId);
      spotAllocationRecordsByVirtualInstanceId.put(virtualInstanceId, spotAllocationRecord);
    }
    return spotAllocationRecordsByVirtualInstanceId;
  }

  /**
   * Returns the Spot allocation record corresponding to the specified virtual instance ID.
   *
   * @param virtualInstanceId the virtual instance ID
   * @return the Spot allocation record corresponding to the specified virtual instance ID
   */
  @VisibleForTesting
  SpotAllocationRecord getSpotAllocationRecord(String virtualInstanceId) {
    return spotAllocationRecordsByVirtualInstanceId.get(virtualInstanceId);
  }

  /**
   * Allocates Spot instances.
   *
   * @throws InterruptedException if the operation is interrupted
   */
  @Override
  @VisibleForTesting
  public Collection<EC2Instance> allocate() throws InterruptedException {

    int expectedInstanceCount = virtualInstanceIds.size();

    LOG.info(">> Requesting {} Spot instances for {}", expectedInstanceCount, template);

    boolean success = false;

    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();

    try {
      try {
        // Check for existing instances orphaned by a previous call.
        checkForOrphanedInstances();

        // Check for existing Spot instance requests orphaned by a previous call.
        Set<String> orphanedSpotInstanceRequests = checkForOrphanedSpotInstanceRequests();
        Set<String> pendingRequestIds = Sets.newHashSet(orphanedSpotInstanceRequests);

        // Need to do a Spot instance request for any virtual instance ids not already associated
        // with an orphaned instance or Spot instance request. In the normal use case, this will
        // include all the requested virtual instance ids.
        Set<String> virtualInstanceIdsNeedingSpotInstanceRequest =
            determineVirtualInstanceIdsNeedingSpotInstanceRequest();

        if (!virtualInstanceIdsNeedingSpotInstanceRequest.isEmpty()) {

          // Request Spot instances
          Map<String, String> virtualInstanceIdToRequestIds = requestSpotInstances(virtualInstanceIds, accumulator);

          // Tag Spot instance requests with virtual instance IDs
          tagSpotInstanceRequests(virtualInstanceIdToRequestIds);

          // Combine the request ids of the reused orphaned requests and the new requests.
          pendingRequestIds.addAll(virtualInstanceIdToRequestIds.values());
        }

        // Wait for Spot requests to be processed
        waitForSpotInstances(pendingRequestIds, false);

        // Tag all the new instances so that we can easily find them later on.
        tagSpotInstances(DateTime.now().plus(waitUntilStartedMillis));

        // Wait until all of them have a private IP (it should be pretty fast)
        Collection<String> terminatedInstanceIds = waitForPrivateIpAddresses();

        // Remove any instances that have been terminated from our internal record
        for (String terminatedInstanceId : terminatedInstanceIds) {
          spotAllocationRecordsByVirtualInstanceId.remove(terminatedInstanceId);
        }

        // Count the allocated instances
        int allocatedInstanceCount = 0;
        for (SpotAllocationRecord spotAllocationRecord : spotAllocationRecordsByVirtualInstanceId.values()) {
          if ((spotAllocationRecord.ec2InstanceId != null) && spotAllocationRecord.instanceTagged) {
            allocatedInstanceCount++;
          }
        }

        if (allocatedInstanceCount < minCount) {
          LOG.info(">> Failed to acquire required number of Spot Instances "
                  + "(desired {}, required {}, acquired {})", expectedInstanceCount, minCount,
              allocatedInstanceCount);
        } else {
          // Wait until we can "find" all the allocated virtual instance ids
          // This will mitigate, but not remove, the possibility that eventual consistency
          // will cause us to not find the instances we just allocated.
          Collection<String> allocatedVirtualInstances = getVirtualInstanceIdsAllocated();
          int numAllocatedInstances = allocatedVirtualInstances.size();
          Collection<EC2Instance> foundInstances =
              allocationHelper.find(template, allocatedVirtualInstances);
          int numFoundInstances = foundInstances.size();
          Stopwatch stopwatch = Stopwatch.createStarted();
          while (numFoundInstances != numAllocatedInstances &&
              stopwatch.elapsed(TimeUnit.MILLISECONDS) < waitUntilFindableMillis) {
            LOG.info("Found {} Spot instances while expecting {}. Waiting for all Spot " +
                    "instances to be findable",
                numFoundInstances, numAllocatedInstances);
            TimeUnit.SECONDS.sleep(5);
            foundInstances = allocationHelper.find(template, allocatedVirtualInstances);
            numFoundInstances = foundInstances.size();
          }
          if (numFoundInstances == numAllocatedInstances) {
            LOG.info("Found all {} allocated Spot instances.", numAllocatedInstances);
          } else {
            LOG.warn("Found only {} of {} Spot instances before wait timeout of {} ms. " +
                    "Continuing anyway.",
                numFoundInstances, numAllocatedInstances, waitUntilFindableMillis);
            LOG.debug("Expecting {}. Found {}.", allocatedVirtualInstances, foundInstances);
          }

          success = true;
          return foundInstances;
        }
      } finally {
        try {
          cancelSpotRequests(accumulator);
        } finally {
          terminateSpotInstances(success, accumulator);
        }
      }
    } catch (AmazonClientException e) {
      // Log here so we get a full stack trace.
      LOG.error("Problem allocating Spot instances", e);
      throw AWSExceptions.propagate(stsClient, e);
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      // Log here so we get a full stack trace.
      LOG.error("Problem allocating Spot instances", e);
      accumulator.addError(null, getErrorMessage(e));
    }

    if (accumulator.hasError()) {
      PluginExceptionDetails pluginExceptionDetails =
          new PluginExceptionDetails(accumulator.getConditionsByKey());
      throw new UnrecoverableProviderException("Problem allocating Spot instances.",
          pluginExceptionDetails);
    }

    return Collections.emptyList();
  }

  @Override
  public void delete() throws InterruptedException {
    allocationHelper.delete(template, virtualInstanceIds);
  }

  /**
   * <p>Identifies reusable Spot instances orphaned by a previous call.</p>
   * <p><em>Note:</em> because of AWS's eventual consistency policies, we are not guaranteed
   * to be able to detect all orphans here, but we make a best-faith effort.</p>
   *
   * @throws InterruptedException if operation is interrupted
   */
  private void checkForOrphanedInstances()
      throws InterruptedException {

    LOG.info(">> Checking for orphaned Spot instances");
    for (Map.Entry<String, Instance> virtualInstanceIdToInstance :
        allocationHelper.doFind(template, virtualInstanceIds)) {
      String ec2InstanceId = virtualInstanceIdToInstance.getValue().getInstanceId();
      String virtualInstanceId = virtualInstanceIdToInstance.getKey();
      LOG.info(">> Found orphaned instance {} / {}; will reuse", ec2InstanceId, virtualInstanceId);
      SpotAllocationRecord spotAllocationRecord = getSpotAllocationRecord(virtualInstanceId);
      spotAllocationRecord.ec2InstanceId = ec2InstanceId;
      spotAllocationRecord.instanceTagged = true;
      spotAllocationRecord.privateIpAddress =
          EC2Instance.getPrivateIpAddress(virtualInstanceIdToInstance.getValue());
    }
  }

  /**
   * Identifies reusable Spot instance requests orphaned by a previous call.
   *
   * @return the reusable Spot instance requests orphaned by a previous call
   */
  private Set<String> checkForOrphanedSpotInstanceRequests() {

    Set<String> orphanedSpotInstanceRequests = Sets.newHashSet();

    LOG.info(">> Checking for orphaned Spot instance requests");
    String idTagName = ec2TagHelper.getClouderaDirectorIdTagName();
    DescribeSpotInstanceRequestsRequest describeSpotInstanceRequestsRequest =
        new DescribeSpotInstanceRequestsRequest().withFilters(
            new Filter()
                .withName("tag:" + idTagName)
                .withValues(virtualInstanceIds));
    DescribeSpotInstanceRequestsResult describeSpotInstanceRequestsResult =
        ec2Client.describeSpotInstanceRequests(describeSpotInstanceRequestsRequest);
    for (SpotInstanceRequest existingSpotInstanceRequest :
        describeSpotInstanceRequestsResult.getSpotInstanceRequests()) {
      String spotInstanceRequestId = existingSpotInstanceRequest.getSpotInstanceRequestId();
      String virtualInstanceId = null;
      for (Tag tag : existingSpotInstanceRequest.getTags()) {
        if (idTagName.equals(tag.getKey())) {
          virtualInstanceId = tag.getValue();
        }
      }
      if (virtualInstanceId == null) {
        LOG.warn(">> Orphaned Spot instance request {} has no virtual instance id",
            spotInstanceRequestId);
      } else {
        SpotAllocationRecord spotAllocationRecord = getSpotAllocationRecord(virtualInstanceId);
        SpotInstanceState spotInstanceState =
            SpotInstanceState.fromValue(existingSpotInstanceRequest.getState());
        switch (spotInstanceState) {
          case Active:
            spotAllocationRecord.spotInstanceRequestId = spotInstanceRequestId;
            String ec2InstanceId = existingSpotInstanceRequest.getInstanceId();
            LOG.info(">> Reusing fulfilled orphaned Spot instance request {} / {} / {}",
                spotInstanceRequestId, virtualInstanceId, ec2InstanceId);
            if (spotAllocationRecord.ec2InstanceId == null) {
              spotAllocationRecord.ec2InstanceId = ec2InstanceId;
            }
            break;
          case Cancelled:
          case Closed:
          case Failed:
            break;
          default:
            if (existingSpotInstanceRequest.getValidUntil().getTime() > System.currentTimeMillis()) {
              LOG.info(">> Reusing pending orphaned Spot instance request {} / {}",
                  spotInstanceRequestId, virtualInstanceId);
              spotAllocationRecord.spotInstanceRequestId = spotInstanceRequestId;
            }
            break;
        }
      }
    }

    return orphanedSpotInstanceRequests;
  }

  /**
   * Determines which virtual instance IDs require a Spot instance request.
   *
   * @return the virtual instance IDs which require a Spot instance request
   */
  @SuppressWarnings("PMD.UselessParentheses")
  private Set<String> determineVirtualInstanceIdsNeedingSpotInstanceRequest() {

    Set<String> result = Sets.newHashSet();

    LOG.info(">> Determining which virtual instances require Spot instance requests");
    for (Map.Entry<String, SpotAllocationRecord> entry
        : spotAllocationRecordsByVirtualInstanceId.entrySet()) {
      SpotAllocationRecord spotAllocationRecord = entry.getValue();
      if ((spotAllocationRecord.ec2InstanceId == null)
          && (spotAllocationRecord.spotInstanceRequestId == null)) {
        result.add(entry.getKey());
      }
    }

    return result;
  }

  /**
   * Builds a {@code RequestSpotInstancesRequest}.
   *
   * @return the {@code RequestSpotInstancesRequest}
   */
  private RequestSpotInstancesRequest newRequestSpotInstanceRequest(String virtualInstanceId) {

    String image = template.getImage();
    String type = template.getType();

    InstanceNetworkInterfaceSpecification network =
        allocationHelper.getInstanceNetworkInterfaceSpecification(template);

    List<BlockDeviceMapping> deviceMappings = allocationHelper.getBlockDeviceMappings(template);

    LaunchSpecification launchSpecification = new LaunchSpecification()
        .withImageId(image)
        .withInstanceType(type)
        .withNetworkInterfaces(network)
        .withBlockDeviceMappings(deviceMappings)
        .withEbsOptimized(template.isEbsOptimized());

    if (template.getIamProfileName().isPresent()) {
      launchSpecification.withIamInstanceProfile(new IamInstanceProfileSpecification()
          .withName(template.getIamProfileName().get()));
    }

    if (template.getKeyName().isPresent()) {
      launchSpecification.withKeyName(template.getKeyName().get());
    }

    SpotPlacement placement = null;
    if (template.getAvailabilityZone().isPresent()) {
      placement = new SpotPlacement().withAvailabilityZone(template.getAvailabilityZone().get());
    }
    if (template.getPlacementGroup().isPresent()) {
      placement = (placement == null) ?
          new SpotPlacement().withGroupName(template.getPlacementGroup().get())
          : placement.withGroupName(template.getPlacementGroup().get());
    }
    launchSpecification.withPlacement(placement);

    Optional<String> userData = template.getUserData();
    if (userData.isPresent()) {
      launchSpecification.withUserData(userData.get());
    }

    LOG.info(">> Spot instance request type: {}, image: {}", type, image);

    @SuppressWarnings("ConstantConditions")
    RequestSpotInstancesRequest request = new RequestSpotInstancesRequest()
        .withLaunchSpecification(launchSpecification)
        .withInstanceCount(1)
        .withClientToken(determineClientToken(virtualInstanceId, requestExpirationTime.getTime()))
        .withValidUntil(requestExpirationTime);
    if (template.getSpotPriceUSDPerHour().isPresent()) {
      request = request.withSpotPrice(template.getSpotPriceUSDPerHour().get().toString());
    }

    Optional<Integer> blockDurationMinutes = template.getBlockDurationMinutes();
    if (blockDurationMinutes.isPresent()) {
      request.withBlockDurationMinutes(blockDurationMinutes.get());
    }

    return request;
  }

  /**
   * Requests Spot instances, and returns the resulting Spot instance request IDs.
   *
   * @param virtualInstanceIds the virtual instance IDs to request spot instances for
   * @param accumulator        plugin exception condition accumulator
   * @return a map of virtual instance ID to spot instance request ID
   */
  private Map<String, String> requestSpotInstances(
      Collection<String> virtualInstanceIds,
      PluginExceptionConditionAccumulator accumulator)
      throws InterruptedException {

    LOG.info(">> Requesting Spot instances");

    Map<String, Future<RequestSpotInstancesResult>> spotResults = Maps.toMap(
        virtualInstanceIds,
        virtualInstanceId -> ec2Client.requestSpotInstancesAsync(newRequestSpotInstanceRequest(virtualInstanceId)));

    Map<String, String> virtualInstanceIdToRequestIds = Maps.newHashMapWithExpectedSize(virtualInstanceIds.size());
    for (Map.Entry<String, Future<RequestSpotInstancesResult>> spotResult : spotResults.entrySet()) {
      try {
        RequestSpotInstancesResult requestSpotInstancesResult = spotResult.getValue().get();
        SpotInstanceRequest requestResponse = getOnlyElement(requestSpotInstancesResult.getSpotInstanceRequests());
        String requestId = requestResponse.getSpotInstanceRequestId();
        LOG.info(">> Created Spot Request {}", requestId);
        virtualInstanceIdToRequestIds.put(spotResult.getKey(), requestId);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof AmazonServiceException) {
          AmazonServiceException awsException = (AmazonServiceException) e.getCause();
          AWSExceptions.propagateIfUnrecoverable(stsClient, awsException);

          String message = "Exception while trying to allocate instance.";

          if (MAX_SPOT_INSTANCE_COUNT_EXCEEDED.equals(awsException.getErrorCode())) {
            message = "Some spot instances were not allocated due to reaching the max spot instance request limit.";
          } else if (AWSExceptions.INSUFFICIENT_INSTANCE_CAPACITY.equals(awsException.getErrorCode()) ||
              AWSExceptions.INSTANCE_LIMIT_EXCEEDED.equals(awsException.getErrorCode())) {
            message = "Some instances were not allocated due to instance limits or capacity issues.";
          } else if (AWSExceptions.REQUEST_LIMIT_EXCEEDED.equals(awsException.getErrorCode())) {
            message = "Encountered rate limit errors while allocating instances.";
          }

          accumulator.addWarning(null, message);
          LOG.warn(message);

        } else {
          LOG.error("Error while requesting spot instance. Attempting to proceed.");
          LOG.debug("Exception caught:", e);
        }
      }
    }

    int lostInstances = virtualInstanceIds.size() - virtualInstanceIdToRequestIds.size();
    if (lostInstances > 0) {
      LOG.warn("Lost {} spot requests.", lostInstances);
    }

    return virtualInstanceIdToRequestIds;
  }

  /**
   * Tags the Spot instance requests with the specified IDs with the corresponding virtual
   * instance IDs.
   *
   * @param virtualInstanceIdToRequestIds map of virtual instance ID to the Spot instance request IDs
   * @throws InterruptedException if the operation is interrupted
   */
  private void tagSpotInstanceRequests(Map<String, String> virtualInstanceIdToRequestIds)
      throws InterruptedException {
    // Pre-compute user-defined tags for efficiency
    List<Tag> userDefinedTags = ec2TagHelper.getUserDefinedTags(template);
    for (Map.Entry<String, String> entry : virtualInstanceIdToRequestIds.entrySet()) {
      String virtualInstanceId = entry.getKey();
      String spotInstanceRequestId = entry.getValue();
      tagSpotInstanceRequest(userDefinedTags, spotInstanceRequestId, virtualInstanceId);
      SpotAllocationRecord spotAllocationRecord = getSpotAllocationRecord(virtualInstanceId);
      spotAllocationRecord.spotInstanceRequestId = spotInstanceRequestId;
    }
  }

  /**
   * Tags an EC2 Spot instance request.
   *
   * @param userDefinedTags       the user-defined tags
   * @param spotInstanceRequestId the Spot instance request ID
   * @param virtualInstanceId     the virtual instance ID
   * @throws InterruptedException if the operation is interrupted
   */
  @SuppressWarnings("PMD.UselessParentheses")
  private void tagSpotInstanceRequest(
      List<Tag> userDefinedTags, final String spotInstanceRequestId, String virtualInstanceId)
      throws InterruptedException {

    LOG.info(">> Tagging Spot instance request {} / {}", spotInstanceRequestId, virtualInstanceId);
    final List<Tag> tags = Lists.newArrayList(
        ec2TagHelper.createClouderaDirectorIdTag(virtualInstanceId),
        ec2TagHelper.createClouderaDirectorTemplateNameTag(template.getName()));
    tags.addAll(userDefinedTags);

    // Test failures and google indicate that we can fail to find a request to tag even when we
    // have determined that it exists by describing it. I am adding a retry loop to attempt to
    // avoid this case.
    try {
      retryUntil(
          (Callable<Void>) () -> {
            ec2Client.createTags(new CreateTagsRequest().withTags(tags).withResources(spotInstanceRequestId));
            return null;
          },
          new DateTime(requestExpirationTime));
    } catch (RetryException e) {
      LOG.warn("timeout waiting for spot instance request {} tagged", spotInstanceRequestId);
    } catch (ExecutionException e) {
      if (AmazonServiceException.class.isInstance(e.getCause())) {
        throw AWSExceptions.propagate(stsClient, (AmazonServiceException) e.getCause());
      }
      throw new UnrecoverableProviderException(e.getCause());
    }
  }

  /**
   * Waits for pending Spot instance requests to be fulfilled.
   *
   * @param pendingRequestIds the pending Spot instance request IDs
   * @param cancelling        whether we are in the process of cancelling
   * @throws InterruptedException if the operation is interrupted
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  private void waitForSpotInstances(Set<String> pendingRequestIds, boolean cancelling)
      throws InterruptedException {

    while (!pendingRequestIds.isEmpty()) {
      // Create the describeRequest object with all of the request ids
      // to monitor (e.g. that we started).
      DescribeSpotInstanceRequestsRequest describeRequest =
          new DescribeSpotInstanceRequestsRequest();
      describeRequest.setSpotInstanceRequestIds(pendingRequestIds);

      // Retrieve all of the requests we want to monitor.
      DescribeSpotInstanceRequestsResult describeResult =
          ec2Client.describeSpotInstanceRequests(describeRequest);
      List<SpotInstanceRequest> describeResponses = describeResult.getSpotInstanceRequests();

      for (SpotInstanceRequest describeResponse : describeResponses) {
        String requestId = describeResponse.getSpotInstanceRequestId();
        SpotInstanceState spotInstanceState =
            SpotInstanceState.fromValue(describeResponse.getState());
        String statusCodeString = describeResponse.getStatus().getCode();
        SpotInstanceRequestStatusCode statusCode =
            SpotInstanceRequestStatusCode.getSpotInstanceStatusCodeByStatusCodeString(
                statusCodeString);
        String virtualInstanceId = null;
        try {
          virtualInstanceId = allocationHelper.getVirtualInstanceId(
              describeResponse.getTags(), "Spot instance request");
        } catch (IllegalStateException ignore) {
          // Tagging is asynchronous. We may get here before the tagging completes.
        }
        switch (spotInstanceState) {
          case Active:
            if (cancelling) {
              LOG.info(">> Waiting, requestId {}, state {}...", requestId, spotInstanceState);
            } else {
              if (virtualInstanceId == null) {
                LOG.info(">> Waiting, requestId {} not yet tagged...", requestId);
              } else {
                pendingRequestIds.remove(requestId);
                SpotAllocationRecord spotAllocationRecord =
                    getSpotAllocationRecord(virtualInstanceId);
                if (spotAllocationRecord.ec2InstanceId == null) {
                  spotAllocationRecord.ec2InstanceId = describeResponse.getInstanceId();
                }
              }
            }
            break;
          case Cancelled:
            pendingRequestIds.remove(requestId);
            switch (statusCode) {
              case REQUEST_CANCELED_AND_INSTANCE_RUNNING:
                if (virtualInstanceId == null) {
                  String ec2InstanceId = describeResponse.getInstanceId();
                  LOG.info(">> Untagged requestId {} has associated instance {}...", requestId,
                      ec2InstanceId);
                  spotInstancesByUntaggedSpotInstanceRequestId.put(requestId, ec2InstanceId);
                } else {
                  SpotAllocationRecord spotAllocationRecord =
                      getSpotAllocationRecord(virtualInstanceId);
                  if (spotAllocationRecord.ec2InstanceId == null) {
                    spotAllocationRecord.ec2InstanceId = describeResponse.getInstanceId();
                  }
                }
                break;
              default:
                break;
            }
            break;
          case Closed:
          case Failed:
            pendingRequestIds.remove(requestId);
            break;
          default:
            switch (statusCode) {
              case PRICE_TOO_LOW:
                if (System.currentTimeMillis() >= priceChangeDeadlineTime.getTime()) {
                  LOG.info("<< Spot price too low for requestId {}", requestId);
                  pendingRequestIds.remove(requestId);
                }
                break;
              default:
                // Keep looping on Open responses
                LOG.info(">> Waiting, requestId {}, state {}...", requestId, spotInstanceState);
                break;
            }
            break;
        }
      }

      if (System.currentTimeMillis() > requestExpirationTime.getTime()) {
        break;
      }

      // TODO add configurable delay
      Thread.sleep(1000);
    }
  }

  /**
   * Tags provisioned Spot instances. Expects that the instances already exists or are in the
   * process of being created. Instances that are not started before the timeout expires are
   * not tagged.
   *
   * @param timeout the time point of timeout
   * @throws InterruptedException if the operation is interrupted
   */
  @SuppressWarnings("PMD.UselessParentheses")
  @VisibleForTesting
  void tagSpotInstances(DateTime timeout) throws InterruptedException {
    // Pre-compute user-defined tags for efficiency
    List<Tag> userDefinedTags = ec2TagHelper.getUserDefinedTags(template);

    for (SpotAllocationRecord spotAllocationRecord :
        spotAllocationRecordsByVirtualInstanceId.values()) {
      if ((spotAllocationRecord.ec2InstanceId != null) && !spotAllocationRecord.instanceTagged &&
          tagInstance(template, userDefinedTags, spotAllocationRecord.virtualInstanceId,
              spotAllocationRecord.ec2InstanceId, timeout)) {
        spotAllocationRecord.instanceTagged = true;
      }
    }
  }


  /**
   * Waits for provisioned Spot instances to have a private IP address.
   *
   * @return virtual instance Ids which have not been assigned private IP addresses
   * @throws InterruptedException if the operation is interrupted
   */
  @SuppressWarnings("PMD.UselessParentheses")
  private Collection<String> waitForPrivateIpAddresses() throws InterruptedException {
    Map<String, String> virtualInstanceIdToEC2InstanceIds = Maps.newHashMap();
    for (SpotAllocationRecord spotAllocationRecord : spotAllocationRecordsByVirtualInstanceId.values()) {
      String ec2InstanceId = spotAllocationRecord.ec2InstanceId;
      if (spotAllocationRecord.privateIpAddress == null &&
          ec2InstanceId != null &&
          spotAllocationRecord.instanceTagged) {
        virtualInstanceIdToEC2InstanceIds.put(spotAllocationRecord.virtualInstanceId, ec2InstanceId);
      }
    }

    return Sets.difference(
        virtualInstanceIdToEC2InstanceIds.keySet(),
        waitForPrivateIpAddresses(virtualInstanceIdToEC2InstanceIds).keySet());
  }

  private Collection<String> getVirtualInstanceIdsAllocated() {
    Set<String> virtualInstanceIds = Sets.newHashSet();

    for (Map.Entry<String, SpotAllocationRecord> entry : spotAllocationRecordsByVirtualInstanceId.entrySet()) {
      String virtualInstanceId = entry.getKey();
      SpotAllocationRecord spotAllocationRecord = entry.getValue();

      String ec2InstanceId = spotAllocationRecord.ec2InstanceId;
      if (ec2InstanceId != null && spotAllocationRecord.instanceTagged) {
        virtualInstanceIds.add(virtualInstanceId);
      }
    }
    return virtualInstanceIds;
  }

  /**
   * Terminates Spot instances (includes discovered orphans and allocated instances). Only
   * untagged instances are terminated if allocation was successful. All instances are terminated
   * if allocation was unsuccessful.
   *
   * @param success     flag indicating whether the allocation was successful
   * @param accumulator the exception condition accumulator
   * @throws InterruptedException if operation is interrupted
   */
  private void terminateSpotInstances(boolean success, PluginExceptionConditionAccumulator accumulator)
      throws InterruptedException {

    Set<String> ec2InstanceIds = Sets.newHashSet();

    if (success) {
      LOG.info("Allocation successful. Cleaning up untagged instances");
      for (SpotAllocationRecord spotAllocationRecord :
          spotAllocationRecordsByVirtualInstanceId.values()) {
        String ec2InstanceId = spotAllocationRecord.ec2InstanceId;
        if (ec2InstanceId != null && !spotAllocationRecord.instanceTagged) {
          ec2InstanceIds.add(ec2InstanceId);
        }
      }
    } else {
      LOG.info("Allocation unsuccessful. Cleaning up all instances");
      for (SpotAllocationRecord spotAllocationRecord :
          spotAllocationRecordsByVirtualInstanceId.values()) {
        String ec2InstanceId = spotAllocationRecord.ec2InstanceId;
        if (ec2InstanceId != null) {
          ec2InstanceIds.add(ec2InstanceId);
        }
      }
    }

    // Instances whose request was untagged should always be terminated
    ec2InstanceIds.addAll(spotInstancesByUntaggedSpotInstanceRequestId.values());

    if (!ec2InstanceIds.isEmpty()) {
      LOG.info(">> Terminating Spot instances {}", ec2InstanceIds);
      TerminateInstancesResult terminateResult;
      try {
        terminateResult = ec2Client.terminateInstances(
            new TerminateInstancesRequest().withInstanceIds(ec2InstanceIds));
        LOG.info("<< Result {}", terminateResult);
      } catch (AmazonClientException e) {
        throw AWSExceptions.propagate(stsClient, e);
      } catch (Exception e) {
        accumulator.addError(null, "Problem terminating Spot instances: "
            + getErrorMessage(e));
      }
    }
  }

  /**
   * Cancels any outstanding Spot requests (includes discovered orphans and created requests).
   *
   * @param accumulator the exception condition accumulator
   * @throws InterruptedException if the operation is interrupted
   */
  private void cancelSpotRequests(PluginExceptionConditionAccumulator accumulator)
      throws InterruptedException {

    final Set<String> spotInstanceRequestIds = Sets.newHashSet();
    for (SpotAllocationRecord spotAllocationRecord :
        spotAllocationRecordsByVirtualInstanceId.values()) {
      String spotInstanceRequestId = spotAllocationRecord.spotInstanceRequestId;
      if (spotInstanceRequestId != null) {
        spotInstanceRequestIds.add(spotInstanceRequestId);
      }
    }

    if (!spotInstanceRequestIds.isEmpty()) {
      try {
        LOG.info(">> Canceling Spot instance requests {}", spotInstanceRequestIds);

        try {
          retryUntil(
              () -> {
                CancelSpotInstanceRequestsRequest request = new CancelSpotInstanceRequestsRequest()
                    .withSpotInstanceRequestIds(spotInstanceRequestIds);
                return ec2Client.cancelSpotInstanceRequests(request);
              },
              new DateTime(requestExpirationTime));
        } catch (RetryException e) {
          LOG.warn("timeout canceling spot request {}", spotInstanceRequestIds);
        } catch (ExecutionException e) {
          throw (Exception) e.getCause();
        }

        waitForSpotInstances(spotInstanceRequestIds, true);
      } catch (AmazonClientException e) {
        throw AWSExceptions.propagate(stsClient, e);
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        accumulator.addError(null, "Problem canceling Spot instance requests: "
            + getErrorMessage(e));
      }
    }
  }
}
