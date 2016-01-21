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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IAM_PROFILE_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.PLACEMENT_GROUP;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_SIZE_GB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SPOT_BID_USD_PER_HR;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;
import static com.cloudera.director.spi.v1.model.util.Validations.addError;

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
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates EC2 instance template configuration.
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "PMD.UnusedPrivateField", "PMD.UnusedPrivateField",
    "unused", "PMD.UselessParentheses"})
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
  static final String INVALID_IAM_PROFILE_NAME_MSG = "Invalid IAM instance profile name: %s";

  private static final String INVALID_SUBNET_ID = "InvalidSubnetID";
  @VisibleForTesting
  static final String INVALID_SUBNET_MSG = "Invalid subnet ID: %s";

  private static final String INVALID_SECURITY_GROUP = "InvalidGroupId";
  @VisibleForTesting
  static final String INVALID_SECURITY_GROUP_MSG = "Invalid security group ID: %s";

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
  static final String IMAGE_OWNER_ID_BLACKLIST_KEY = "ownerId";

  @VisibleForTesting
  static final String IMAGE_SPOT_OWNER_ID_BLACKLIST_KEY = "spotOwnerId";

  @VisibleForTesting
  static final String IMAGE_PLATFORM_BLACKLIST_KEY = "platform";

  @VisibleForTesting
  static final String IMAGE_SPOT_PLATFORM_BLACKLIST_KEY = "spotPlatform";

  /**
   * The EC2 provider.
   */
  private final EC2Provider provider;

  /**
   * The template filters.
   */
  private final AWSFilters templateFilters;

  /**
   * Creates an EC2 instance template configuration validator with the specified parameters.
   *
   * @param provider the EC2 provider
   */
  public EC2InstanceTemplateConfigurationValidator(EC2Provider provider) {
    this.provider = Preconditions.checkNotNull(provider, "provider");
    templateFilters = provider.getEC2Filters().getSubfilters("template");
  }

  @Override
  public void validate(String name, Configured configuration,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {

    AmazonEC2Client ec2Client = provider.getClient();

    checkImage(ec2Client, configuration, accumulator, localizationContext);
    checkSubnetId(ec2Client, configuration, accumulator, localizationContext);
    checkSecurityGroupIds(ec2Client, configuration, accumulator, localizationContext);
    checkAvailabilityZone(ec2Client, configuration, accumulator, localizationContext);
    checkPlacementGroup(ec2Client, configuration, accumulator, localizationContext);
    checkIamProfileName(configuration, accumulator, localizationContext);
    checkRootVolumeSize(configuration, accumulator, localizationContext);
    checkRootVolumeType(configuration, accumulator, localizationContext);
    checkKeyName(ec2Client, configuration, accumulator, localizationContext);
    checkSpotParameters(configuration, accumulator, localizationContext);
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
   */
  @VisibleForTesting
  void checkSubnetId(AmazonEC2Client client,
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
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().startsWith(INVALID_SUBNET_ID)) {
        addError(accumulator, SUBNET_ID, localizationContext,
            null, INVALID_SUBNET_MSG, subnetId);
      } else {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Validates the configured security group IDs.
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkSecurityGroupIds(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    List<String> securityGroupsIds = EC2InstanceTemplate.CSV_SPLITTER.splitToList(
        configuration.getConfigurationValue(SECURITY_GROUP_IDS, localizationContext));

    for (String securityGroupId : securityGroupsIds) {
      LOG.info(">> Describing security group '{}'", securityGroupId);

      try {
        DescribeSecurityGroupsResult result = client.describeSecurityGroups(
            new DescribeSecurityGroupsRequest().withGroupIds(securityGroupId));
        checkCount(accumulator, SECURITY_GROUP_IDS, localizationContext, securityGroupId,
            result.getSecurityGroups()
        );
      } catch (AmazonServiceException e) {
        if (e.getErrorCode().startsWith(INVALID_SECURITY_GROUP)) {
          addError(accumulator, SECURITY_GROUP_IDS, localizationContext,
              null, INVALID_SECURITY_GROUP_MSG, securityGroupId);
        } else {
          throw Throwables.propagate(e);
        }
      }
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
