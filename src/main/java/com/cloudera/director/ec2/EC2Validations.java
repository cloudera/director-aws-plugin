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

package com.cloudera.director.ec2;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesRequest;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsRequest;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribePlacementGroupsRequest;
import com.amazonaws.services.ec2.model.DescribePlacementGroupsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various AWS specific validations
 */
public class EC2Validations {

  private static final Logger LOG = LoggerFactory.getLogger(EC2Validations.class);

  @VisibleForTesting
  static final int MIN_ROOT_VOLUME_SIZE_GB = 10;
  private static final String SIXTY_FOUR_BIT_ARCHITECTURE = "x86_64";
  private static final String AVAILABLE_STATE = "available";

  @VisibleForTesting
  static final String HVM_VIRTUALIZATION = "hvm";

  @VisibleForTesting
  static final String PARAVIRTUAL_VIRTUALIZATION = "paravirtual";

  @VisibleForTesting
  static final String ROOT_DEVICE_TYPE = "ebs";

  @VisibleForTesting
  static final Set<String> ROOT_VOLUME_TYPES = ImmutableSet.of("gp2", "standard");

  @VisibleForTesting
  static final String INVALID_AMI_NAME = "AMI ID does not start with ami-";
  private static final String INVALID_AMI_ID = "InvalidAMIID";
  @VisibleForTesting
  static final String INVALID_AMI = "Invalid AMI: %s";
  @VisibleForTesting
  static final String INVALID_AMI_ARCHITECTURE = "Only 64-bit architecture is supported. Invalid architecture for AMI %s: %s";
  @VisibleForTesting
  static final String INVALID_AMI_PLATFORM = "Only Linux platforms are supported. Invalid platform for AMI %s: %s";
  private static final String INVALID_AMI_STATE = "AMI should be available. Invalid state for AMI %s: %s";
  @VisibleForTesting
  static final String INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY = "Incompatible AMI virtualization type. " +
      "Instance type %s does not support %s virtualization type of AMI %s.";
  @VisibleForTesting
  static final String INVALID_AMI_ROOT_DEVICE_TYPE = "Only EBS root device type is supported. " +
      "Invalid root device type for AMI %s: %s";

  private static final String INVALID_PARAMETER_VALUE = "InvalidParameterValue";

  private static final String INVALID_AVAILABILITY_ZONE = "Invalid availability zone";
  @VisibleForTesting
  static final String INVALID_AVAILABILITY_ZONE_MSG = INVALID_AVAILABILITY_ZONE + " : %s";

  private static final String INVALID_PLACEMENT_GROUP_ID = "InvalidPlacementGroup";
  @VisibleForTesting
  static final String INVALID_PLACEMENT_GROUP = "Invalid placement group: %s";

  @VisibleForTesting
  static final String INVALID_IAM_PROFILE_NAME = "Invalid IAM instance profile name: %s";

  private static final String INVALID_SUBNET_ID = "InvalidSubnetID";
  @VisibleForTesting
  static final String INVALID_SUBNET = "Invalid subnet ID: %s";

  private static final String INVALID_SECURITY_GROUP = "InvalidGroupId";
  @VisibleForTesting
  static final String INVALID_SECURITY_GROUP_MSG = "Invalid security group ID: %s";

  private static final String INVALID_KEY_PAIR = "InvalidKeyPair";
  @VisibleForTesting
  static final String INVALID_KEY_NAME = "Invalid key name: %s";

  private static final String INVALID_COUNT_EMPTY = "%s not found: %s";
  private static final String INVALID_COUNT_DUPLICATES = "More than one %s found with identifier %s";

  private final VirtualizationMappings virtualizationMappings;

  /**
   * Creates a new validator.
   *
   * @param virtualizationMappings image virtualization mappings
   * @throws NullPointerException if virtualizationMappings is null
   */
  public EC2Validations(VirtualizationMappings virtualizationMappings) {
    this.virtualizationMappings =
        checkNotNull(virtualizationMappings, "virtualizationMappings is null");
  }

  /**
   * Validate that all the template identifiers reference existing resources
   *
   * @param provider EC2 provider
   * @param template Template with configuration parameters to create EC2 instances
   * @return List of validation errors
   */
  public Set<String> validateTemplate(EC2Provider provider, EC2InstanceTemplate template) {
    checkNotNull(provider, "client is null");

    AmazonEC2Client ec2 = provider.getClient();
    Set<String> validationErrors = Sets.newLinkedHashSet();

    validationErrors.addAll(checkImage(ec2, template.getImage(), template.getType()));
    validationErrors.addAll(checkSubnetId(ec2, template.getSubnetId()));
    validationErrors.addAll(checkSecurityGroupsIds(ec2, template.getSecurityGroupIds()));

    if (template.getAvailabilityZone().isPresent()) {
      validationErrors.addAll(checkAvailabilityZone(ec2, template.getAvailabilityZone().get()));
    }
    if (template.getPlacementGroup().isPresent()) {
      validationErrors.addAll(checkPlacementGroup(ec2, template.getPlacementGroup().get()));
    }
    if (template.getIamProfileName().isPresent()) {
      AmazonIdentityManagement iam = provider.getIdentityManagementClient();
      validationErrors.addAll(checkIamProfileName(iam, template.getIamProfileName().get()));
    }

    validationErrors.addAll(checkRootVolumeSize(template.getRootVolumeSizeGB()));
    validationErrors.addAll(checkRootVolumeType(template.getRootVolumeType()));
    return validationErrors;
  }

  /**
   * Check image ID by querying the AWS API
   */
  @VisibleForTesting
  List<String> checkImage(AmazonEC2Client client, String imageName, String type) {
    List<String> validationErrors = Lists.newArrayList();
    if (!imageName.startsWith("ami-")) {
      validationErrors.add(INVALID_AMI_NAME);
      return validationErrors;
    }

    LOG.info(">> Describing AMI '{}'", imageName);
    DescribeImagesResult result;
    try {
      result = client.describeImages(
          new DescribeImagesRequest().withImageIds(imageName));
      validationErrors.addAll(checkCount(result.getImages(), "AMI", imageName));
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().startsWith(INVALID_AMI_ID)) {
        return Lists.newArrayList(String.format(INVALID_AMI, imageName));
      }
      throw Throwables.propagate(e);
    }

    if (!validationErrors.isEmpty()) {
      return validationErrors;
    }

    Image image = Iterables.getOnlyElement(result.getImages());
    if (!SIXTY_FOUR_BIT_ARCHITECTURE.equals(image.getArchitecture())) {
      validationErrors.add(String.format(INVALID_AMI_ARCHITECTURE,
          imageName, image.getArchitecture()));
    }

    if (image.getPlatform() != null) {
      // Platform is Windows for Windows and blank for everything else
      validationErrors.add(String.format(INVALID_AMI_PLATFORM,
          imageName, image.getPlatform()));
    }

    if (!AVAILABLE_STATE.equals(image.getState())) {
      validationErrors.add(String.format(INVALID_AMI_STATE,
          imageName, image.getState()));
    }

    if (!virtualizationMappings.apply(image.getVirtualizationType())
        .contains(type)) {
      validationErrors.add(String.format(INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY,
          type, image.getVirtualizationType(), imageName));
    }

    if (!ROOT_DEVICE_TYPE.equals(image.getRootDeviceType())) {
      validationErrors.add(String.format(INVALID_AMI_ROOT_DEVICE_TYPE,
          imageName, image.getRootDeviceType()));
    }

    return validationErrors;
  }

  /**
   * Check the availability zone
   */
  @VisibleForTesting
  List<String> checkAvailabilityZone(AmazonEC2Client client, String zoneName) {
    LOG.info(">> Describing zone '{}'", zoneName);

    try {
      DescribeAvailabilityZonesResult result = client.describeAvailabilityZones(
          new DescribeAvailabilityZonesRequest().withZoneNames(zoneName));

      return checkCount(result.getAvailabilityZones(), "Availablility zone", zoneName);
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().equals(INVALID_PARAMETER_VALUE) &&
          e.getMessage().contains(INVALID_AVAILABILITY_ZONE)) {
        return Lists.newArrayList(String.format(INVALID_AVAILABILITY_ZONE_MSG, zoneName));
      }
      throw Throwables.propagate(e);
    }
  }

  /**
   * Check validity of placement group name
   */
  @VisibleForTesting
  List<String> checkPlacementGroup(AmazonEC2Client client, String placementGroup) {
    LOG.info(">> Describing placement group '{}'", placementGroup);

    try {
      DescribePlacementGroupsResult result = client.describePlacementGroups(
          new DescribePlacementGroupsRequest().withGroupNames(placementGroup));

      return checkCount(result.getPlacementGroups(), "Placement group", placementGroup);
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().startsWith(INVALID_PLACEMENT_GROUP_ID)) {
        return Lists.newArrayList(String.format(INVALID_PLACEMENT_GROUP, placementGroup));
      }
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  List<String> checkIamProfileName(AmazonIdentityManagement iam, String iamProfileName) {
    try {
      iam.getInstanceProfile(new GetInstanceProfileRequest()
          .withInstanceProfileName(iamProfileName));
      return Collections.emptyList();

    } catch (NoSuchEntityException e) {
      return ImmutableList.of(String.format(INVALID_IAM_PROFILE_NAME, iamProfileName));
    }
  }

  /**
   * Check validity of the subnet ID
   */
  @VisibleForTesting
  List<String> checkSubnetId(AmazonEC2Client client, String subnetId) {
    LOG.info(">> Describing subnet '{}'", subnetId);

    try {
      DescribeSubnetsResult result = client.describeSubnets(
          new DescribeSubnetsRequest().withSubnetIds(subnetId));
      return checkCount(result.getSubnets(), "Subnet", subnetId);
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().startsWith(INVALID_SUBNET_ID)) {
        return Lists.newArrayList(String.format(INVALID_SUBNET, subnetId));
      }
      throw Throwables.propagate(e);
    }
  }

  /**
   * Check validity of a comma separated list of security IDs
   */
  @VisibleForTesting
  List<String> checkSecurityGroupsIds(AmazonEC2Client client, List<String> securityGroupsIds) {
    List<String> validationErrors = Lists.newArrayList();

    for (String securityGroupId : securityGroupsIds) {
      LOG.info(">> Describing security group '{}'", securityGroupId);

      try {
        DescribeSecurityGroupsResult result = client.describeSecurityGroups(
            new DescribeSecurityGroupsRequest().withGroupIds(securityGroupId));
        validationErrors.addAll(
            checkCount(result.getSecurityGroups(), securityGroupId, "Security group"));
      } catch (AmazonServiceException e) {
        if (e.getErrorCode().startsWith(INVALID_SECURITY_GROUP)) {
          validationErrors.add(String.format(INVALID_SECURITY_GROUP_MSG, securityGroupId));
        } else {
          throw Throwables.propagate(e);
        }
      }
    }

    return validationErrors;
  }

  /**
   * Check rootVolumeSizeGB is valid
   */
  @VisibleForTesting
  List<String> checkRootVolumeSize(int rootVolumeSizeGB) {
    List<String> validationErrors = Lists.newArrayList();

    if (rootVolumeSizeGB < MIN_ROOT_VOLUME_SIZE_GB) {
      validationErrors.add(String.format(
          "Root volume size should be at least %dGB. Current configuration: %dGB",
          MIN_ROOT_VOLUME_SIZE_GB, rootVolumeSizeGB));
    }

    return validationErrors;
  }

  @VisibleForTesting
  List<String> checkRootVolumeType(String rootVolumeType) {
    List<String> validationErrors = Lists.newArrayList();

    if (!ROOT_VOLUME_TYPES.contains(rootVolumeType)) {
      validationErrors.add(String.format("Invalid root volume type %s. Available options: %s",
          rootVolumeType, Joiner.on(", ").join(ROOT_VOLUME_TYPES)));
    }

    return validationErrors;
  }

  /**
   * Validate the EC2 key name
   */
  List<String> validateKeyName(AmazonEC2Client client, String keyName) {
    LOG.info(">> Describing key pair");
    try {
      DescribeKeyPairsResult result = client.describeKeyPairs(
          new DescribeKeyPairsRequest().withKeyNames(keyName));
      return checkCount(result.getKeyPairs(), "NotDisplayed", "Key name");

    } catch (AmazonServiceException e) {
      if (e.getErrorCode().startsWith(INVALID_KEY_PAIR)) {
        return Lists.newArrayList(String.format(INVALID_KEY_NAME, keyName));
      }
      throw Throwables.propagate(e);
    }
  }

  private <T> List<String> checkCount(List<T> result, String field, String fieldDescriptor) {
    List<String> validationErrors = Lists.newArrayList();
    if (result.isEmpty()) {
      validationErrors.add(String.format(INVALID_COUNT_EMPTY, fieldDescriptor, field));
    }

    if (result.size() > 1) {
      validationErrors.add(
          String.format(INVALID_COUNT_DUPLICATES, fieldDescriptor, field));
    }

    return validationErrors;
  }
}
