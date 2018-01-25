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

package com.cloudera.director.aws.ec2;


import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.AVAILABILITY_ZONE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.BLOCK_DURATION_MINUTES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_IOPS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_KMS_KEY_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_COUNT;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_SIZE_GIB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ENCRYPT_ADDITIONAL_EBS_VOLUMES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IAM_PROFILE_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.PLACEMENT_GROUP;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_SIZE_GB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SPOT_BID_USD_PER_HR;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TENANCY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USER_DATA;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USER_DATA_UNENCODED;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;
import static com.cloudera.director.spi.v2.model.util.Validations.addError;

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
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DescribeKeyRequest;
import com.amazonaws.services.kms.model.NotFoundException;
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata.EbsVolumeMetadata;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates EC2 instance template configuration.
 */
@SuppressWarnings({ "PMD.TooManyStaticImports", "PMD.UnusedPrivateField", "PMD.UnusedPrivateField",
    "unused", "PMD.UselessParentheses" })
public class EC2InstanceTemplateConfigurationValidator implements ConfigurationValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(EC2InstanceTemplateConfigurationValidator.class);

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
  static final Set<String> TENANCY_TYPES = ImmutableSet.of("default", "dedicated");

  @VisibleForTesting
  static final String INVALID_AMI_NAME_MSG = "AMI ID does not start with ami-: %s";
  private static final String INVALID_AMI_ID = "InvalidAMIID";
  @VisibleForTesting
  static final String INVALID_AMI_MSG = "Invalid AMI: %s";
  @VisibleForTesting
  static final String INVALID_AMI_ARCHITECTURE_MSG =
      "Only 64-bit architecture is supported. Invalid architecture for AMI %s: %s";
  @VisibleForTesting
  static final String INVALID_AMI_OWNER_MSG =
      "Only certain Linux platforms are supported. "
          + "See the \"Supported Distributions and Resource Requirements\" section of "
          + "the Cloudera Director User Guide. "
          + "Invalid owner Id for AMI %s: %s (%s)";
  @VisibleForTesting
  static final String INVALID_AMI_OWNER_SPOT_MSG =
      "Only certain Linux platforms are supported for use with Spot instances. "
          + "See the \"Supported Distributions and Resource Requirements\" section of "
          + "the Cloudera Director User Guide. "
          + "Invalid owner Id for AMI %s: %s (%s)";
  @VisibleForTesting
  static final String INVALID_AMI_PLATFORM_MSG =
      "Only certain Linux platforms are supported. "
          + "See the \"Supported Distributions and Resource Requirements\" section of "
          + "the Cloudera Director User Guide. "
          + "Invalid platform for AMI %s: %s (%s)";
  @VisibleForTesting
  static final String INVALID_AMI_PLATFORM_SPOT_MSG =
      "Only certain Linux platforms are supported for use with Spot instances. "
          + "See the \"Supported Distributions and Resource Requirements\" section of "
          + "the Cloudera Director User Guide. "
          + "Invalid platform for AMI %s: %s (%s)";
  private static final String INVALID_AMI_STATE_MSG =
      "AMI should be available. Invalid state for AMI %s: %s";
  @VisibleForTesting
  static final String INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY_MSG =
      "Incompatible AMI virtualization type." +
          " Instance type %s does not support %s virtualization type of AMI %s.";
  @VisibleForTesting
  static final String INVALID_AMI_ROOT_DEVICE_TYPE_MSG = "Only EBS root device type is supported." +
      " Invalid root device type for AMI %s: %s";

  private static final String INVALID_PARAMETER_VALUE = "InvalidParameterValue";

  private static final String INVALID_AVAILABILITY_ZONE = "Invalid availability zone";
  @VisibleForTesting
  static final String INVALID_AVAILABILITY_ZONE_MSG = INVALID_AVAILABILITY_ZONE + " : %s";

  private static final String INVALID_PLACEMENT_GROUP_ID = "InvalidPlacementGroup";
  @VisibleForTesting
  static final String INVALID_PLACEMENT_GROUP_MSG = "Invalid placement group: %s";

  @VisibleForTesting
  static final String INVALID_TENANCY_MSG = "Invalid tenancy type: %s. Available options: %s";

  @VisibleForTesting
  static final String INVALID_IAM_PROFILE_NAME_MSG = "Invalid IAM instance profile name: %s";

  private static final String INVALID_SUBNET_ID = "InvalidSubnetID";
  @VisibleForTesting
  static final String INVALID_SUBNET_MSG = "Invalid subnet ID: %s";

  static final String INVALID_SECURITY_GROUP = "InvalidGroup";
  @VisibleForTesting
  static final String INVALID_SECURITY_GROUP_MSG = "Invalid security group ID: %s";

  @VisibleForTesting
  static final String INVALID_SECURITY_GROUP_VPC_MSG =
      "Security group %s and subnet %s belong to different networks.";

  private static final String INVALID_KEY_PAIR = "InvalidKeyPair";
  @VisibleForTesting
  static final String INVALID_KEY_NAME_MSG = "Invalid key name: %s";

  public static final String INVALID_ROOT_VOLUME_TYPE_MSG =
      "Invalid root volume type %s. Available options: %s";
  public static final String INVALID_ROOT_VOLUME_SIZE_FORMAT_MSG =
      "Root volume size must be an integer: %s";
  public static final String INVALID_ROOT_VOLUME_SIZE_MSG =
      "Root volume size should be at least %dGB. Current configuration: %dGB";

  private static final String INVALID_COUNT_EMPTY_MSG = "%s not found: %s";
  private static final String INVALID_COUNT_DUPLICATES_MSG =
      "More than one %s found with identifier %s";

  @VisibleForTesting
  static final String INVALID_SPOT_BID_MSG =
      "Invalid Spot bid %s. Spot bid must be a positive value representing the price in USD/hr";

  @VisibleForTesting
  static final String INVALID_BLOCK_DURATION_MINUTES_MSG =
      "Invalid block duration in minutes, %s. Block duration must be a multiple of 60 " +
          "(60, 120, 180, 240, 300, or 360)";

  @VisibleForTesting
  static final String IMAGE_OWNER_ID_BLACKLIST_KEY = "ownerId";

  @VisibleForTesting
  static final String IMAGE_SPOT_OWNER_ID_BLACKLIST_KEY = "spotOwnerId";

  @VisibleForTesting
  static final String IMAGE_PLATFORM_BLACKLIST_KEY = "platform";

  @VisibleForTesting
  static final String IMAGE_SPOT_PLATFORM_BLACKLIST_KEY = "spotPlatform";

  @VisibleForTesting
  static final int MAX_VOLUMES_PER_INSTANCE = 10;

  @VisibleForTesting
  static final String INVALID_EBS_VOLUME_COUNT_FORMAT_MSG =
      "EBS volume count must be a integer: %s";

  @VisibleForTesting
  static final String INVALID_EBS_VOLUME_COUNT_MSG =
      "EBS volume count must be a non-negative integer no greater than %s";

  @VisibleForTesting
  static final String INVALID_EBS_VOLUME_SIZE_FORMAT_MSG =
      "EBS volume size must be a positive integer: %s";

  @VisibleForTesting
  static final String VOLUME_SIZE_NOT_IN_RANGE_MSG =
      "Volume size for %s must be between %d GiB and %d GiB";

  @VisibleForTesting
  static final String IOPS_NOT_PERMITTED_MSG = "IOPS should only be set for io1 volume type";

  @VisibleForTesting
  static final String IOPS_REQUIRED_MSG = "IOPS must be set for io1 volume type";

  @VisibleForTesting
  static final String INVALID_IOPS_FORMAT_MSG = "IOPS must be a positive integer";

  @VisibleForTesting
  static final String IOPS_NOT_IN_RANGE_MSG =
      "IOPS of %d for %s is not in range, it must be between %d and %d";

  @VisibleForTesting
  static final String INVALID_EBS_ENCRYPTION_MSG =
      "EBS volume count should be greater than 0 to specify EBS encryption properties";

  @VisibleForTesting
  static final String INVALID_KMS_WHEN_ENCRYPTION_DISABLED_MSG =
      "The KMS Key ID can only be set with encryption enabled";

  @VisibleForTesting
  static final String INVALID_KMS_NOT_FOUND_MESSAGE =
      "The KMS Key ID could not be found";

  @VisibleForTesting
  static final String KMS_KEY_DENIED_MESSAGE =
      "Access denied attempting to verify the KMS Key ID. Ensure kms:DescribeKey permission is granted";

  @VisibleForTesting
  static final String BOTH_USER_DATA_USED =
      "Specify only the encoded or unencoded user data, not both";

  /**
   * The EC2 provider.
   */
  private final EC2Provider provider;

  /**
   * The template filters.
   */
  private final AWSFilters templateFilters;

  /**
   * The EBS metadata.
   */
  private final EBSMetadata ebsMetadata;

  /**
   * Creates an EC2 instance template configuration validator with the specified parameters.
   *
   * @param provider the EC2 provider
   */
  public EC2InstanceTemplateConfigurationValidator(EC2Provider provider, EBSMetadata ebsMetadata) {
    this.provider = Preconditions.checkNotNull(provider, "provider");
    this.ebsMetadata = Preconditions.checkNotNull(ebsMetadata, "ebsMetadata");
    templateFilters = provider.getEC2Filters().getSubfilters("template");
  }

  @Override
  public void validate(String name, Configured configuration,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {

    AmazonEC2Client ec2Client = provider.getClient();
    AWSKMSClient kmsClient = provider.getKmsClient();

    checkImage(ec2Client, configuration, accumulator, localizationContext);
    Map<String, String> vpcSubnetMap = checkSubnetId(ec2Client, configuration, accumulator, localizationContext);
    Map<String, Set<String>> vpcSecurityGroupMap =
        checkSecurityGroupIds(ec2Client, configuration, accumulator, localizationContext);
    checkVpc(vpcSubnetMap, vpcSecurityGroupMap, accumulator, localizationContext);
    checkAvailabilityZone(ec2Client, configuration, accumulator, localizationContext);
    checkPlacementGroup(ec2Client, configuration, accumulator, localizationContext);
    checkTenancy(configuration, accumulator, localizationContext);
    checkIamProfileName(configuration, accumulator, localizationContext);
    checkRootVolumeSize(configuration, accumulator, localizationContext);
    checkRootVolumeType(configuration, accumulator, localizationContext);
    checkEbsVolumes(kmsClient, configuration, accumulator, localizationContext);
    checkKeyName(ec2Client, configuration, accumulator, localizationContext);
    checkSpotParameters(configuration, accumulator, localizationContext);
    checkUserData(configuration, accumulator, localizationContext);
  }

  /**
   * Validates the configured AMI.
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  @SuppressWarnings("PMD.CollapsibleIfStatements")
  void checkImage(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String imageName = configuration.getConfigurationValue(IMAGE, localizationContext);
    String type = configuration.getConfigurationValue(TYPE, localizationContext);

    int conditionCount = accumulator.getConditionsByKey().size();

    if (!imageName.startsWith("ami-")) {
      addError(accumulator, IMAGE, localizationContext,
          null, INVALID_AMI_NAME_MSG, imageName);
      return;
    }

    LOG.info(">> Describing AMI '{}'", imageName);
    DescribeImagesResult result = null;
    try {
      result = client.describeImages(
          new DescribeImagesRequest().withImageIds(imageName));
      checkCount(accumulator, IMAGE, localizationContext, imageName,
          result.getImages());
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().startsWith(INVALID_AMI_ID)) {
        addError(accumulator, IMAGE, localizationContext,
            null, INVALID_AMI_MSG, imageName);
      } else {
        throw Throwables.propagate(e);
      }
    }

    if ((result == null) || (accumulator.getConditionsByKey().size() > conditionCount)) {
      return;
    }

    Image image = Iterables.getOnlyElement(result.getImages());
    if (!SIXTY_FOUR_BIT_ARCHITECTURE.equals(image.getArchitecture())) {
      addError(accumulator, IMAGE, localizationContext,
          null, INVALID_AMI_ARCHITECTURE_MSG, imageName, image.getArchitecture());
    }

    AWSFilters imageFilters = templateFilters.getSubfilters(IMAGE.unwrap().getConfigKey());
    boolean useSpotInstances = Boolean.parseBoolean(
        configuration.getConfigurationValue(USE_SPOT_INSTANCES, localizationContext));

    String ownerId = image.getOwnerId();
    if (ownerId != null) {
      String blacklistValue = imageFilters.getBlacklistValue(
          IMAGE_OWNER_ID_BLACKLIST_KEY, ownerId.toLowerCase());
      if (blacklistValue != null) {
        addError(accumulator, IMAGE, localizationContext,
            null, INVALID_AMI_OWNER_MSG, imageName, ownerId, blacklistValue);
      } else {
        if (useSpotInstances) {
          blacklistValue = imageFilters.getBlacklistValue(
              IMAGE_SPOT_OWNER_ID_BLACKLIST_KEY, ownerId.toLowerCase());
          if (blacklistValue != null) {
            addError(accumulator, IMAGE, localizationContext,
                null, INVALID_AMI_OWNER_SPOT_MSG, imageName, ownerId, blacklistValue);
          }
        }
      }
    }

    String platform = image.getPlatform();
    if (platform != null) {
      String blacklistValue = imageFilters.getBlacklistValue(
          IMAGE_PLATFORM_BLACKLIST_KEY, platform.toLowerCase());
      if (blacklistValue != null) {
        addError(accumulator, IMAGE, localizationContext,
            null, INVALID_AMI_PLATFORM_MSG, imageName, platform, blacklistValue);
      } else {
        if (useSpotInstances) {
          blacklistValue = imageFilters.getBlacklistValue(
              IMAGE_SPOT_PLATFORM_BLACKLIST_KEY, platform.toLowerCase());
          if (blacklistValue != null) {
            addError(accumulator, IMAGE, localizationContext,
                null, INVALID_AMI_PLATFORM_SPOT_MSG, imageName, platform, blacklistValue);
          }
        }
      }
    }

    if (!AVAILABLE_STATE.equals(image.getState())) {
      addError(accumulator, IMAGE, localizationContext,
          null, INVALID_AMI_STATE_MSG, imageName, image.getState());
    }

    if (!provider.getVirtualizationMappings().apply(image.getVirtualizationType()).contains(type)) {
      addError(accumulator, IMAGE, localizationContext,
          null, INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY_MSG,
          type, image.getVirtualizationType(), imageName);
    }

    if (!ROOT_DEVICE_TYPE.equals(image.getRootDeviceType())) {
      addError(accumulator, IMAGE, localizationContext,
          null, INVALID_AMI_ROOT_DEVICE_TYPE_MSG, imageName, image.getRootDeviceType());
    }
  }

  /**
   * Validates the configured availability zone.
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkAvailabilityZone(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String zoneName = configuration.getConfigurationValue(AVAILABILITY_ZONE, localizationContext);
    if (zoneName != null) {
      LOG.info(">> Describing zone '{}'", zoneName);

      try {
        DescribeAvailabilityZonesResult result = client.describeAvailabilityZones(
            new DescribeAvailabilityZonesRequest().withZoneNames(zoneName));

        checkCount(accumulator, AVAILABILITY_ZONE, localizationContext, "Availability zone",
            result.getAvailabilityZones());
      } catch (AmazonServiceException e) {
        if (e.getErrorCode().equals(INVALID_PARAMETER_VALUE) &&
            e.getMessage().contains(INVALID_AVAILABILITY_ZONE)) {
          addError(accumulator, AVAILABILITY_ZONE, localizationContext,
              null, INVALID_AVAILABILITY_ZONE_MSG, zoneName);
        } else {
          throw Throwables.propagate(e);
        }
      }
    }
  }

  /**
   * Validates the configured placement group.
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkPlacementGroup(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String placementGroup =
        configuration.getConfigurationValue(PLACEMENT_GROUP, localizationContext);

    if (placementGroup != null) {
      LOG.info(">> Describing placement group '{}'", placementGroup);

      try {
        DescribePlacementGroupsResult result = client.describePlacementGroups(
            new DescribePlacementGroupsRequest().withGroupNames(placementGroup));

        checkCount(accumulator, PLACEMENT_GROUP, localizationContext, "Placement group",
            result.getPlacementGroups());
      } catch (AmazonServiceException e) {
        if (e.getErrorCode().startsWith(INVALID_PLACEMENT_GROUP_ID)) {
          addError(accumulator, PLACEMENT_GROUP, localizationContext,
              null, INVALID_PLACEMENT_GROUP_MSG, placementGroup);
        } else {
          throw Throwables.propagate(e);
        }
      }
    }
  }
  /**
   * Validates the configured tenancy type.
   *
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkTenancy(Configured configuration,
                           PluginExceptionConditionAccumulator accumulator,
                           LocalizationContext localizationContext) {

    String tenancy =
            configuration.getConfigurationValue(TENANCY, localizationContext);

    if (!TENANCY_TYPES.contains(tenancy)) {
      addError(accumulator, TENANCY, localizationContext,
              null, INVALID_TENANCY_MSG,
              tenancy, Joiner.on(", ").join(TENANCY_TYPES));
    }
  }

  /**
   * Validates the configured IAM profile.
   *
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkIamProfileName(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String iamProfileName =
        configuration.getConfigurationValue(IAM_PROFILE_NAME, localizationContext);

    if (iamProfileName != null) {
      AmazonIdentityManagementClient iamClient = provider.getIdentityManagementClient();

      try {
        iamClient.getInstanceProfile(new GetInstanceProfileRequest()
            .withInstanceProfileName(iamProfileName));

      } catch (NoSuchEntityException e) {
        addError(accumulator, IAM_PROFILE_NAME, localizationContext,
            null, INVALID_IAM_PROFILE_NAME_MSG, iamProfileName);
      }
    }
  }

  /**
   * Validates the configured subnet ID.
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   *
   * @return the vpc id to subnet id mapping
   */
  @VisibleForTesting
  Map<String, String> checkSubnetId(AmazonEC2Client client,
                                    Configured configuration,
                                    PluginExceptionConditionAccumulator accumulator,
                                    LocalizationContext localizationContext) {
    String subnetId = configuration.getConfigurationValue(SUBNET_ID, localizationContext);
    LOG.info(">> Describing subnet '{}'", subnetId);

    try {
      DescribeSubnetsResult result = client.describeSubnets(
          new DescribeSubnetsRequest().withSubnetIds(subnetId));
      checkCount(accumulator, SUBNET_ID, localizationContext, "Subnet",
          result.getSubnets());
      if (result.getSubnets().size() == 1) {
        return ImmutableMap.of(Iterables.getOnlyElement(result.getSubnets()).getVpcId(), subnetId);
      }
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().startsWith(INVALID_SUBNET_ID)) {
        addError(accumulator, SUBNET_ID, localizationContext,
            null, INVALID_SUBNET_MSG, subnetId);
      } else {
        throw Throwables.propagate(e);
      }
    }
    return ImmutableMap.of();
  }

  /**
   * Validates the configured security group IDs.
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   *
   * @return the vpc id to security group ids mapping
   */
  @VisibleForTesting
  Map<String, Set<String>> checkSecurityGroupIds(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    List<String> securityGroupsIds = EC2InstanceTemplate.CSV_SPLITTER.splitToList(
        configuration.getConfigurationValue(SECURITY_GROUP_IDS, localizationContext));

    Map<String, Set<String>> vpcSgMap = Maps.newHashMap();
    for (String securityGroupId : securityGroupsIds) {
      LOG.info(">> Describing security group '{}'", securityGroupId);

      try {
        DescribeSecurityGroupsResult result = client.describeSecurityGroups(
            new DescribeSecurityGroupsRequest().withGroupIds(securityGroupId));
        checkCount(accumulator, SECURITY_GROUP_IDS, localizationContext, securityGroupId,
            result.getSecurityGroups()
        );
        if (result.getSecurityGroups().size() == 1) {
          String vpcId = Iterables.getOnlyElement(result.getSecurityGroups()).getVpcId();
          Set<String> sgSet = vpcSgMap.get(vpcId);
          if (sgSet == null) {
            sgSet = Sets.newHashSet();
            vpcSgMap.put(vpcId, sgSet);
          }
          sgSet.add(securityGroupId);
        }
      } catch (AmazonServiceException e) {
        if (e.getErrorCode().startsWith(INVALID_SECURITY_GROUP)) {
          addError(accumulator, SECURITY_GROUP_IDS, localizationContext,
              null, INVALID_SECURITY_GROUP_MSG, securityGroupId);
        } else {
          throw Throwables.propagate(e);
        }
      }
    }
    return vpcSgMap;
  }

  /**
   * Validates given security groups and subnet are in the same vpc.
   */
  @VisibleForTesting
  void checkVpc(Map<String, String> vpcSubnetMap,
                Map<String, Set<String>> vpcSecurityGroupMap,
                PluginExceptionConditionAccumulator accumulator,
                LocalizationContext localizationContext) {
    if (vpcSubnetMap.size() != 1) {
      LOG.error("Skipping vpc validation due to subnet validation error");
      return;
    }

    if (vpcSecurityGroupMap.isEmpty()) {
      LOG.error("Skipping vpc validation due to security group validation error");
      return;
    }

    Map.Entry<String, String> vpcSubnetEntry = Iterables.getOnlyElement(vpcSubnetMap.entrySet());
    String vpcId = vpcSubnetEntry.getKey();
    String subnetId = vpcSubnetEntry.getValue();

    Set<String> vpcDiff = Sets.difference(vpcSecurityGroupMap.keySet(), ImmutableSet.of(vpcId));
    if (!vpcDiff.isEmpty()) {
      Set<String> invalidSgs = Sets.newHashSet();
      for (String invalidVpcId : vpcDiff) {
        invalidSgs.addAll(vpcSecurityGroupMap.get(invalidVpcId));
      }
      addError(accumulator, SECURITY_GROUP_IDS, localizationContext,
          null, INVALID_SECURITY_GROUP_VPC_MSG, invalidSgs, subnetId
      );
    }
  }

  /**
   * Validates the configured root volume size.
   *
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkRootVolumeSize(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String rootVolumeSizeGBString =
        configuration.getConfigurationValue(ROOT_VOLUME_SIZE_GB, localizationContext);

    try {
      int rootVolumeSizeGB = Integer.parseInt(rootVolumeSizeGBString);
      if (rootVolumeSizeGB < MIN_ROOT_VOLUME_SIZE_GB) {
        addError(accumulator, ROOT_VOLUME_SIZE_GB, localizationContext,
            null, INVALID_ROOT_VOLUME_SIZE_MSG, MIN_ROOT_VOLUME_SIZE_GB, rootVolumeSizeGB);
      }
    } catch (NumberFormatException e) {
      addError(accumulator, ROOT_VOLUME_SIZE_GB, localizationContext,
          null, INVALID_ROOT_VOLUME_SIZE_FORMAT_MSG, rootVolumeSizeGBString);
    }
  }

  /**
   * Validates the configured root volume type.
   *
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkRootVolumeType(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String rootVolumeType =
        configuration.getConfigurationValue(ROOT_VOLUME_TYPE, localizationContext);

    if (!ROOT_VOLUME_TYPES.contains(rootVolumeType)) {
      addError(accumulator, ROOT_VOLUME_TYPE, localizationContext,
          null, INVALID_ROOT_VOLUME_TYPE_MSG,
          rootVolumeType, Joiner.on(", ").join(ROOT_VOLUME_TYPES));
    }
  }

  /**
   * Validates the configuration for EBS volumes.
   *
   * @param kmsClient           the AWS KMS client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkEbsVolumes(AWSKMSClient kmsClient, Configured configuration,
                       PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {
    String ebsVolumeCountString = configuration.getConfigurationValue(EBS_VOLUME_COUNT, localizationContext);

    int ebsVolumeCount;
    try {
      ebsVolumeCount = Integer.parseInt(ebsVolumeCountString);
    } catch (NumberFormatException e) {
      addError(accumulator, EBS_VOLUME_COUNT, localizationContext,
        null, INVALID_EBS_VOLUME_COUNT_FORMAT_MSG, ebsVolumeCountString);
      return;
    }

    if (ebsVolumeCount < 0 || ebsVolumeCount > MAX_VOLUMES_PER_INSTANCE) {
      addError(accumulator, EBS_VOLUME_COUNT, localizationContext,
          null, INVALID_EBS_VOLUME_COUNT_MSG, MAX_VOLUMES_PER_INSTANCE);
      return;
    }

    boolean enableEbsEncryption;

    enableEbsEncryption = Boolean.parseBoolean(
        configuration.getConfigurationValue(ENCRYPT_ADDITIONAL_EBS_VOLUMES, localizationContext));

    String kmsKeyId = configuration.getConfigurationValue(EBS_KMS_KEY_ID, localizationContext);

    if (ebsVolumeCount == 0) {

      // Disallow setting any EBS encryption configuration when not adding EBS
      // volumes. This makes it more apparent that encryption is done on the
      // added EBS volumes and not the root.

      if (enableEbsEncryption) {
        addError(accumulator, ENCRYPT_ADDITIONAL_EBS_VOLUMES, localizationContext, null, INVALID_EBS_ENCRYPTION_MSG);
      }

      if (kmsKeyId != null) {
        addError(accumulator, EBS_KMS_KEY_ID, localizationContext, null, INVALID_EBS_ENCRYPTION_MSG);
      }
    }

    if (ebsVolumeCount > 0) {

      if (kmsKeyId != null) {
        if (!enableEbsEncryption) {
          addError(accumulator, EBS_KMS_KEY_ID, localizationContext, null, INVALID_KMS_WHEN_ENCRYPTION_DISABLED_MSG);
        }
        // verify that we can find the key in KMS
        DescribeKeyRequest keyRequest = new DescribeKeyRequest().withKeyId(kmsKeyId);
        try {
          kmsClient.describeKey(keyRequest);
        } catch (NotFoundException ex) {
          addError(accumulator, EBS_KMS_KEY_ID, localizationContext, null, INVALID_KMS_NOT_FOUND_MESSAGE);
        } catch (AmazonServiceException ex) {
          if (ex.getErrorCode().equals("AccessDeniedException")) {
            addError(accumulator, EBS_KMS_KEY_ID, localizationContext, null, KMS_KEY_DENIED_MESSAGE);
          } else {
            addError(accumulator, EBS_KMS_KEY_ID, localizationContext, null,
                "AmazonServiceException exception " + ex.getErrorMessage());
          }
        }
      }

      String volumeType = configuration.getConfigurationValue(EBS_VOLUME_TYPE, localizationContext);
      EbsVolumeMetadata metadata;

      try {
        metadata = ebsMetadata.apply(volumeType);
      } catch (NullPointerException e) {
        addError(accumulator, EBS_VOLUME_TYPE, localizationContext, null, "Volume type unknown: " + e.getMessage());
        return;
      } catch (IllegalStateException e) {
        addError(accumulator, EBS_VOLUME_TYPE, localizationContext, null, "Malformed metadata: " + e.getMessage());
        return;
      }

      String strEbsVolumeSizeGiB = configuration.getConfigurationValue(EBS_VOLUME_SIZE_GIB, localizationContext);
      int ebsVolumeSizeGiB;
      try {
        ebsVolumeSizeGiB = Integer.parseInt(strEbsVolumeSizeGiB);
      } catch (NumberFormatException e) {
        addError(accumulator, EBS_VOLUME_SIZE_GIB, localizationContext,
            null, INVALID_EBS_VOLUME_SIZE_FORMAT_MSG, strEbsVolumeSizeGiB);
        return;
      }

      int minAllowableSize = metadata.getMinSizeGiB();
      int maxAllowableSize = metadata.getMaxSizeGiB();

      if (ebsVolumeSizeGiB > maxAllowableSize || ebsVolumeSizeGiB < minAllowableSize) {
        addError(accumulator, EBS_VOLUME_SIZE_GIB, localizationContext,
            null, VOLUME_SIZE_NOT_IN_RANGE_MSG, volumeType, minAllowableSize, maxAllowableSize);
      }

      checkeEbsIops(accumulator, localizationContext, configuration, metadata);
    }
  }

  private void checkeEbsIops(PluginExceptionConditionAccumulator accumulator,
                             LocalizationContext localizationContext, Configured configuration,
                             EbsVolumeMetadata metadata) {

    String volumeType = configuration.getConfigurationValue(EBS_VOLUME_TYPE, localizationContext);
    String strEbsIops = configuration.getConfigurationValue(EBS_IOPS, localizationContext);

    if (!volumeType.equals("io1")) {
      if (strEbsIops != null) {
        addError(accumulator, EBS_IOPS, localizationContext, null, IOPS_NOT_PERMITTED_MSG);
      }
      return;
    }

    if (strEbsIops == null) {
      addError(accumulator, EBS_IOPS, localizationContext, null, IOPS_REQUIRED_MSG);
      return;
    }

    int ebsIopsCount;
    try {
      ebsIopsCount = Integer.parseInt(strEbsIops);
    } catch (NumberFormatException e) {
      addError(accumulator, EBS_IOPS, localizationContext, null, INVALID_IOPS_FORMAT_MSG);
      return;
    }

    if (metadata.getMinIops().isPresent() && metadata.getMaxIops().isPresent()) {
      int minAllowableIops = metadata.getMinIops().get();
      int maxAllowableIops = metadata.getMaxIops().get();

      if (ebsIopsCount > maxAllowableIops || ebsIopsCount < minAllowableIops) {
        addError(accumulator, EBS_IOPS, localizationContext,
            null, IOPS_NOT_IN_RANGE_MSG, ebsIopsCount, volumeType, minAllowableIops, maxAllowableIops);
      }
    }
  }

  /**
   * Validates the EC2 key name.
   *
   * @param client              the EC2 client
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkKeyName(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String keyName = configuration.getConfigurationValue(KEY_NAME, localizationContext);

    if (keyName != null) {
      LOG.info(">> Describing key pair");
      try {
        DescribeKeyPairsResult result = client.describeKeyPairs(
            new DescribeKeyPairsRequest().withKeyNames(keyName));
        // TODO Should this be REDACTED instead of NotDisplayed?
        checkCount(accumulator, KEY_NAME, localizationContext, "NotDisplayed",
            result.getKeyPairs());

      } catch (AmazonServiceException e) {
        if (e.getErrorCode().startsWith(INVALID_KEY_PAIR)) {
          addError(accumulator, KEY_NAME, localizationContext,
              null, INVALID_KEY_NAME_MSG, keyName);
        } else {
          throw Throwables.propagate(e);
        }
      }
    }
  }

  /**
   * Validates the configured Spot parameters.
   *
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  @SuppressWarnings("PMD.EmptyCatchBlock")
  void checkSpotParameters(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    boolean useSpotInstances = Boolean.parseBoolean(
        configuration.getConfigurationValue(USE_SPOT_INSTANCES, localizationContext));
    String spotBidUSDPerHr =
        configuration.getConfigurationValue(SPOT_BID_USD_PER_HR, localizationContext);
    String blockDurationMinutes = Strings.emptyToNull(
        configuration.getConfigurationValue(BLOCK_DURATION_MINUTES, localizationContext));

    if ((spotBidUSDPerHr == null) || spotBidUSDPerHr.isEmpty()) {
      if (useSpotInstances) {
        addError(accumulator, SPOT_BID_USD_PER_HR, localizationContext,
            null, SPOT_BID_USD_PER_HR.unwrap().getMissingValueErrorMessage(localizationContext));
      }
    } else {
      boolean valid = false;
      try {
        BigDecimal spotBid = new BigDecimal(spotBidUSDPerHr);
        valid = spotBid.compareTo(BigDecimal.ZERO) > 0;
      } catch (NumberFormatException ignore) {
      }
      if (!valid) {
        addError(accumulator, SPOT_BID_USD_PER_HR, localizationContext,
            null, INVALID_SPOT_BID_MSG, spotBidUSDPerHr);
      }
    }
    if (blockDurationMinutes != null) {
      boolean valid = false;
      try {
        Integer blockDuration = Integer.valueOf(blockDurationMinutes);
        valid = blockDuration >= 60 &&
            blockDuration <= 360 &&
            blockDuration % 60 == 0;
      } catch (NumberFormatException ignore) {
      }
      if (!valid) {
        addError(accumulator, BLOCK_DURATION_MINUTES, localizationContext,
            null, INVALID_BLOCK_DURATION_MINUTES_MSG, blockDurationMinutes);
      }
    }
  }


  /**
   * Validates that only one user data property was used, if any.
   *
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkUserData(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String userData = configuration.getConfigurationValue(USER_DATA, localizationContext);
    String userDataUnencoded = configuration.getConfigurationValue(USER_DATA_UNENCODED, localizationContext);
    if (userData != null && userDataUnencoded != null) {
      addError(accumulator, USER_DATA, localizationContext,
          null, BOTH_USER_DATA_USED);
    }
  }

  /**
   * Verifies that the specified result list has exactly one element, and reports an appropriate
   * error otherwise.
   *
   * @param accumulator         the exception condition accumulator
   * @param token               the token representing the configuration property in error
   * @param localizationContext the localization context
   * @param field               the problem field value
   * @param result              the result list to be validated
   */
  private void checkCount(PluginExceptionConditionAccumulator accumulator,
      ConfigurationPropertyToken token, LocalizationContext localizationContext,
      String field, List<?> result) {

    if (result.isEmpty()) {
      addError(accumulator, token, localizationContext,
          null, INVALID_COUNT_EMPTY_MSG, token.unwrap().getName(localizationContext), field);
    }

    if (result.size() > 1) {
      addError(accumulator, token, localizationContext,
          null, INVALID_COUNT_DUPLICATES_MSG, token.unwrap().getName(localizationContext), field);
    }
  }
}
