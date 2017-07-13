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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_KMS_KEY_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ENCRYPT_ADDITIONAL_EBS_VOLUMES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_COUNT;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_SIZE_GIB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_TYPE;
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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TENANCY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.HVM_VIRTUALIZATION;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.IMAGE_OWNER_ID_BLACKLIST_KEY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.IMAGE_PLATFORM_BLACKLIST_KEY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.IMAGE_SPOT_OWNER_ID_BLACKLIST_KEY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.IMAGE_SPOT_PLATFORM_BLACKLIST_KEY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_ARCHITECTURE_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_NAME_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_OWNER_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_OWNER_SPOT_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_PLATFORM_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_PLATFORM_SPOT_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_ROOT_DEVICE_TYPE_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_EBS_VOLUME_COUNT_FORMAT_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_EBS_ENCRYPTION_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_EBS_VOLUME_COUNT_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_EBS_VOLUME_SIZE_FORMAT_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_IAM_PROFILE_NAME_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_KMS_NOT_FOUND_MESSAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_KMS_WHEN_ENCRYPTION_DISABLED_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.MAX_VOLUMES_PER_INSTANCE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.MIN_ROOT_VOLUME_SIZE_GB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.PARAVIRTUAL_VIRTUALIZATION;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.ROOT_VOLUME_TYPES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.VOLUME_SIZE_NOT_IN_RANGE_MSG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata.EbsVolumeMetadata;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.AvailabilityZone;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeAvailabilityZonesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeKeyPairsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribePlacementGroupsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribePlacementGroupsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Image;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceType;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.KeyPairInfo;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.PlacementGroup;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.SecurityGroup;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Subnet;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.VolumeType;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.model.GetInstanceProfileResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.cloudera.director.aws.shaded.com.amazonaws.services.kms.AWSKMSClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.kms.model.DescribeKeyRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.kms.model.NotFoundException;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

@SuppressWarnings("PMD.TooManyStaticImports")
public class EC2InstanceTemplateConfigurationValidatorTest {

  private static final String IMAGE_NAME = "ami-1234567";
  private static final InstanceType VALID_INSTANCE_TYPE = InstanceType.T2Micro;
  private static final String TYPE_STRING = VALID_INSTANCE_TYPE.toString();
  private static final String INVALID_PLATFORM = "invalidPlatform";
  private static final String INVALID_SPOT_PLATFORM = "invalidSpotPlatform";
  private static final String INVALID_OWNER_ID = "invalidOwner";
  private static final String INVALID_SPOT_OWNER_ID = "invalidSpotOwner";

  private EC2Provider ec2Provider;
  private EBSMetadata ebsMetadata;
  private VirtualizationMappings virtualizationMappings;
  private EC2InstanceTemplateConfigurationValidator validator;
  private AmazonEC2Client ec2Client;
  private AWSKMSClient kmsClient;
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  @Before
  public void setUp() {
    virtualizationMappings = mock(VirtualizationMappings.class);
    ebsMetadata = mock(EBSMetadata.class);
    AWSFilters ec2Filters = mockEC2Filters();
    ec2Client = mock(AmazonEC2Client.class);
    ec2Provider = mock(EC2Provider.class);
    kmsClient = mock(AWSKMSClient.class);
    when(ec2Provider.getClient()).thenReturn(ec2Client);
    when(ec2Provider.getVirtualizationMappings()).thenReturn(virtualizationMappings);
    when(ec2Provider.getEC2Filters()).thenReturn(ec2Filters);
    validator = new EC2InstanceTemplateConfigurationValidator(ec2Provider, ebsMetadata);
    accumulator = new PluginExceptionConditionAccumulator();
  }

  /**
   * Test that all supported EBS volumes types are known by the AWS SDK
   */
  @Test
  public void testRootVolumeTypes() {
    Set<String> types = Sets.newHashSet();
    for (VolumeType volumeType : VolumeType.values()) {
      types.add(volumeType.name().toLowerCase());
    }

    assertThat(types).containsAll(ROOT_VOLUME_TYPES);
  }

  private AWSFilters mockEC2Filters() {
    AWSFilters ec2Filters = mock(AWSFilters.class);
    AWSFilters imageFilters = mock(AWSFilters.class);
    addMockBlacklist(imageFilters, IMAGE_PLATFORM_BLACKLIST_KEY, INVALID_PLATFORM, "Windows");
    addMockBlacklist(imageFilters, IMAGE_SPOT_PLATFORM_BLACKLIST_KEY, INVALID_SPOT_PLATFORM);
    addMockBlacklist(imageFilters, IMAGE_OWNER_ID_BLACKLIST_KEY, INVALID_OWNER_ID);
    addMockBlacklist(imageFilters, IMAGE_SPOT_OWNER_ID_BLACKLIST_KEY, INVALID_SPOT_OWNER_ID);
    addNestedFilters(ec2Filters, imageFilters, "template", IMAGE.unwrap().getConfigKey());
    return ec2Filters;
  }

  private void addNestedFilters(AWSFilters awsFilters, AWSFilters nestedFilters, String... keys) {
    int stop = keys.length - 1;
    if (stop >= 0) {
      AWSFilters currentFilters = awsFilters;
      for (int i = 0; i < stop; i++) {
        String key = keys[i];
        AWSFilters childFilters = mock(AWSFilters.class);
        when(currentFilters.getSubfilters(key)).thenReturn(childFilters);
        currentFilters = childFilters;
      }
      String key = keys[stop];
      when(currentFilters.getSubfilters(key)).thenReturn(nestedFilters);
    }
  }

  private void addMockBlacklist(AWSFilters awsFilters, String key, String... filterValues) {
    AWSFilters blacklistFilters = mock(AWSFilters.class);

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String filterValue : filterValues) {
      builder.put(filterValue.toLowerCase(), filterValue);
    }
    Map<String, String> filterMap = builder.build();
    when(blacklistFilters.getFilterMap(AWSFilters.BLACKLIST)).thenReturn(filterMap);
    when(awsFilters.getSubfilters(key)).thenReturn(blacklistFilters);
    when(awsFilters.getBlacklistValue(Matchers.eq(key), Matchers.anyString())).thenCallRealMethod();
    when(awsFilters.isBlacklisted(Matchers.eq(key), Matchers.anyString())).thenCallRealMethod();
  }

  private Image mockSingleCheckedImage() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    Image image = mock(Image.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Collections.singletonList(image));
    return image;
  }

  private void mockImageAttributes(Image image, String architecture, String platform,
      String state, String virtualizationType, String rootDeviceType, String ownerId) {
    when(image.getArchitecture()).thenReturn(architecture);
    when(image.getPlatform()).thenReturn(platform);
    when(image.getState()).thenReturn(state);
    when(image.getVirtualizationType()).thenReturn(virtualizationType);
    when(image.getRootDeviceType()).thenReturn(rootDeviceType);
    when(image.getOwnerId()).thenReturn(ownerId);
  }

  @Test
  public void testCheckImage() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "paravirtual", "ebs", null);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    assertThat(accumulator.hasError()).isFalse();
    assertThat(accumulator.hasWarning()).isFalse();

    ArgumentCaptor<DescribeImagesRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeImagesRequest.class);
    verify(ec2Client).describeImages(reqCaptor.capture());
    DescribeImagesRequest req = reqCaptor.getValue();
    assertThat(req.getImageIds()).isEqualTo(Lists.newArrayList(IMAGE_NAME));
  }

  @Test
  public void testCheckImage_NonAMI() {
    String amiName = "xyz-1234567";
    checkImage(amiName, TYPE_STRING, false);
    verifySingleError(IMAGE, INVALID_AMI_NAME_MSG, amiName);
  }

  @Test
  public void testCheckImage_NoAMI() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Collections.<Image>emptyList());

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE);
  }

  @Test
  public void testCheckImage_DuplicateAMI() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    Image image = mock(Image.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Lists.newArrayList(image, image));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE);
  }

  @Test
  public void testCheckImage_Not64Bit() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86", null, "available", "paravirtual", "ebs", null);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE, INVALID_AMI_ARCHITECTURE_MSG, IMAGE_NAME, "x86");
  }

  @Test
  public void testCheckImage_Windows() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", "Windows", "available", "paravirtual", "ebs", null);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE, INVALID_AMI_PLATFORM_MSG, IMAGE_NAME, "Windows", "Windows");
  }

  @Test
  public void testCheckImage_InvalidPlatform() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", INVALID_PLATFORM, "available", "paravirtual", "ebs", null);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE, INVALID_AMI_PLATFORM_MSG, IMAGE_NAME, INVALID_PLATFORM,
        INVALID_PLATFORM);
  }

  @Test
  public void testCheckImage_InvalidSpotPlatform() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", INVALID_SPOT_PLATFORM, "available", "paravirtual", "ebs", null);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, true);
    verifySingleError(IMAGE, INVALID_AMI_PLATFORM_SPOT_MSG, IMAGE_NAME, INVALID_SPOT_PLATFORM,
        INVALID_SPOT_PLATFORM);
  }

  @Test
  public void testCheckImage_InvalidOwner() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "paravirtual", "ebs", INVALID_OWNER_ID);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE, INVALID_AMI_OWNER_MSG, IMAGE_NAME, INVALID_OWNER_ID,
        INVALID_OWNER_ID);
  }

  @Test
  public void testCheckImage_InvalidSpotOwner() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(
        image, "x86_64", null, "available", "paravirtual", "ebs", INVALID_SPOT_OWNER_ID);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, true);
    verifySingleError(
        IMAGE, INVALID_AMI_OWNER_SPOT_MSG, IMAGE_NAME, INVALID_SPOT_OWNER_ID,
        INVALID_SPOT_OWNER_ID);
  }

  @Test
  public void testCheckImage_InvalidState() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "newjersey", "paravirtual", "ebs", null);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE);
  }

  @Test
  public void testCheckImage_IncompatibleInstanceType() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "hvm", "ebs", null);
    when(virtualizationMappings.apply(HVM_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING + "x"));
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE, INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY_MSG, TYPE_STRING, "hvm",
        IMAGE_NAME);
  }

  @Test
  public void testCheckImage_RootDeviceType() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "paravirtual", "instance-store", null);
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING, false);
    verifySingleError(IMAGE, INVALID_AMI_ROOT_DEVICE_TYPE_MSG, IMAGE_NAME, "instance-store");
  }

  @Test
  public void testCheckAvailabilityZone() {
    DescribeAvailabilityZonesResult dazResult = mock(DescribeAvailabilityZonesResult.class);
    AvailabilityZone zone = mock(AvailabilityZone.class);
    when(ec2Client.describeAvailabilityZones(any(DescribeAvailabilityZonesRequest.class)))
        .thenReturn(dazResult);
    when(dazResult.getAvailabilityZones()).thenReturn(Collections.singletonList(zone));

    String zoneName = "zone";
    checkAvailabilityZone(zoneName);
    verifyClean();

    ArgumentCaptor<DescribeAvailabilityZonesRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeAvailabilityZonesRequest.class);
    verify(ec2Client).describeAvailabilityZones(reqCaptor.capture());
    DescribeAvailabilityZonesRequest req = reqCaptor.getValue();
    assertThat(req.getZoneNames()).isEqualTo(Lists.newArrayList(zoneName));
  }

  @Test
  public void testCheckAvailabilityZone_NoZone() {
    DescribeAvailabilityZonesResult dazResult = mock(DescribeAvailabilityZonesResult.class);
    when(ec2Client.describeAvailabilityZones(any(DescribeAvailabilityZonesRequest.class)))
        .thenReturn(dazResult);
    when(dazResult.getAvailabilityZones()).thenReturn(Collections.<AvailabilityZone>emptyList());

    checkAvailabilityZone("zone");
    verifySingleError(AVAILABILITY_ZONE);
  }

  @Test
  public void testCheckAvailabilityZone_DuplicateZone() {
    DescribeAvailabilityZonesResult dazResult = mock(DescribeAvailabilityZonesResult.class);
    AvailabilityZone zone = mock(AvailabilityZone.class);
    when(ec2Client.describeAvailabilityZones(any(DescribeAvailabilityZonesRequest.class)))
        .thenReturn(dazResult);
    when(dazResult.getAvailabilityZones()).thenReturn(Lists.newArrayList(zone, zone));

    checkAvailabilityZone("zone");
    verifySingleError(AVAILABILITY_ZONE);
  }

  @Test
  public void testCheckPlacementGroup() {
    DescribePlacementGroupsResult dpgResult = mock(DescribePlacementGroupsResult.class);
    PlacementGroup group = mock(PlacementGroup.class);
    when(ec2Client.describePlacementGroups(any(DescribePlacementGroupsRequest.class)))
        .thenReturn(dpgResult);
    when(dpgResult.getPlacementGroups()).thenReturn(Collections.singletonList(group));

    String placementGroup = "group";
    checkPlacementGroup(placementGroup);
    verifyClean();

    ArgumentCaptor<DescribePlacementGroupsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribePlacementGroupsRequest.class);
    verify(ec2Client).describePlacementGroups(reqCaptor.capture());
    DescribePlacementGroupsRequest req = reqCaptor.getValue();
    assertThat(req.getGroupNames()).isEqualTo(Lists.newArrayList(placementGroup));
  }

  @Test
  public void testCheckPlacementGroup_NoGroup() {
    DescribePlacementGroupsResult dpgResult = mock(DescribePlacementGroupsResult.class);
    when(ec2Client.describePlacementGroups(any(DescribePlacementGroupsRequest.class)))
        .thenReturn(dpgResult);
    when(dpgResult.getPlacementGroups()).thenReturn(Collections.<PlacementGroup>emptyList());

    checkPlacementGroup("group");
    verifySingleError(PLACEMENT_GROUP);
  }

  @Test
  public void testCheckPlacementGroup_DuplicateGroup() {
    DescribePlacementGroupsResult dpgResult = mock(DescribePlacementGroupsResult.class);
    PlacementGroup group = mock(PlacementGroup.class);
    when(ec2Client.describePlacementGroups(any(DescribePlacementGroupsRequest.class)))
        .thenReturn(dpgResult);
    when(dpgResult.getPlacementGroups()).thenReturn(Lists.newArrayList(group, group));

    checkPlacementGroup("group");
    verifySingleError(PLACEMENT_GROUP);
  }

  @Test
  public void testCheckTenancy_Default() {
    String tenancy = "default";
    checkTenancy(tenancy);
    verifyClean();
  }

  @Test
  public void testCheckTenancy_Dedicated() {
    String tenancy = "dedicated";
    checkTenancy(tenancy);
    verifyClean();
  }

  @Test
  public void testCheckTenancy_Invalid() {
    String tenancy = "some-invalid-tenancy";
    checkTenancy(tenancy);
    verifySingleError(TENANCY);
  }

  @Test
  public void testCheckIamProfileName() {
    AmazonIdentityManagementClient iamClient = mock(AmazonIdentityManagementClient.class);
    GetInstanceProfileResult gipResult = mock(GetInstanceProfileResult.class);
    when(iamClient.getInstanceProfile(any(GetInstanceProfileRequest.class)))
        .thenReturn(gipResult);
    when(ec2Provider.getIdentityManagementClient()).thenReturn(iamClient);

    String iamProfileName = "role";
    checkIamProfileName(iamProfileName);
    verifyClean();

    ArgumentCaptor<GetInstanceProfileRequest> reqCaptor =
        ArgumentCaptor.forClass(GetInstanceProfileRequest.class);
    verify(iamClient).getInstanceProfile(reqCaptor.capture());
    GetInstanceProfileRequest req = reqCaptor.getValue();
    assertThat(req.getInstanceProfileName()).isEqualTo(iamProfileName);
  }

  @Test
  public void testCheckIamProfileName_NoSuchEntity() {
    AmazonIdentityManagementClient iamClient = mock(AmazonIdentityManagementClient.class);
    String iamProfileName = "role";
    when(iamClient.getInstanceProfile(any(GetInstanceProfileRequest.class)))
        .thenThrow(new NoSuchEntityException(iamProfileName));
    when(ec2Provider.getIdentityManagementClient()).thenReturn(iamClient);

    checkIamProfileName(iamProfileName);
    verifySingleError(IAM_PROFILE_NAME, INVALID_IAM_PROFILE_NAME_MSG, iamProfileName);
  }

  @Test
  public void testCheckSubnetId() {
    DescribeSubnetsResult dsResult = mock(DescribeSubnetsResult.class);
    Subnet subnet = mock(Subnet.class);
    when(ec2Client.describeSubnets(any(DescribeSubnetsRequest.class)))
        .thenReturn(dsResult);
    when(dsResult.getSubnets()).thenReturn(Collections.singletonList(subnet));
    when(subnet.getVpcId()).thenReturn("test-vpc-id");

    String subnetId = "subnet";
    checkSubnetId(subnetId);
    verifyClean();

    ArgumentCaptor<DescribeSubnetsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeSubnetsRequest.class);
    verify(ec2Client).describeSubnets(reqCaptor.capture());
    DescribeSubnetsRequest req = reqCaptor.getValue();
    assertThat(req.getSubnetIds()).isEqualTo(Lists.newArrayList(subnetId));
  }

  @Test
  public void testCheckSubnetId_NoSubnet() {
    DescribeSubnetsResult dsResult = mock(DescribeSubnetsResult.class);
    when(ec2Client.describeSubnets(any(DescribeSubnetsRequest.class)))
        .thenReturn(dsResult);
    when(dsResult.getSubnets()).thenReturn(Collections.<Subnet>emptyList());

    checkSubnetId("subnet");
    verifySingleError(SUBNET_ID);
  }

  @Test
  public void testCheckSubnetId_DuplicateSubnet() {
    DescribeSubnetsResult dsResult = mock(DescribeSubnetsResult.class);
    Subnet subnet = mock(Subnet.class);
    when(ec2Client.describeSubnets(any(DescribeSubnetsRequest.class)))
        .thenReturn(dsResult);
    when(dsResult.getSubnets()).thenReturn(Lists.newArrayList(subnet, subnet));

    checkSubnetId("subnet");
    verifySingleError(SUBNET_ID);
  }

  @Test
  public void testCheckSecurityGroupsIds() {
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    SecurityGroup securityGroup = mock(SecurityGroup.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Collections.singletonList(securityGroup));

    Collection<String> securityGroupIds = Collections.singletonList("securityGroup");
    checkSecurityGroupIds(securityGroupIds);
    verifyClean();

    ArgumentCaptor<DescribeSecurityGroupsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeSecurityGroupsRequest.class);
    verify(ec2Client).describeSecurityGroups(reqCaptor.capture());
    DescribeSecurityGroupsRequest req = reqCaptor.getValue();
    assertThat(req.getGroupIds()).isEqualTo(securityGroupIds);
  }

  @Test
  public void testCheckSecurityGroupsIds_NoSecurityGroup() {
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Collections.<SecurityGroup>emptyList());

    checkSecurityGroupIds(Collections.singletonList("securityGroup"));
    verifySingleError(SECURITY_GROUP_IDS);
  }

  @Test
  public void testCheckSecurityGroupsIds_DuplicateSecurityGroup() {
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    SecurityGroup securityGroup = mock(SecurityGroup.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(
        Lists.newArrayList(securityGroup, securityGroup));

    checkSecurityGroupIds(Collections.singletonList("securityGroup"));
    verifySingleError(SECURITY_GROUP_IDS);
  }

  @Test
  public void testCheckVpc() {
    Map<String, String> vpcSubnetMap = ImmutableMap.of("vpc-1111", "subnet-1111");
    Map<String, Set<String>> vpcSgMap = Maps.newHashMap();
    vpcSgMap.put("vpc-1111", ImmutableSet.of("sg-1111", "sg-1112"));
    checkVpc(vpcSubnetMap, vpcSgMap);
    verifyClean();

    vpcSgMap.put("vpc-2222", ImmutableSet.of("sg-2222"));
    checkVpc(vpcSubnetMap, vpcSgMap);
    verifySingleError(SECURITY_GROUP_IDS);
  }

  @Test
  public void testCheckRootVolumeSize() {
    checkRootVolumeSize(MIN_ROOT_VOLUME_SIZE_GB);
    verifyClean();

    checkRootVolumeSize(MIN_ROOT_VOLUME_SIZE_GB - 1);
    verifySingleError(ROOT_VOLUME_SIZE_GB);
  }

  @Test
  public void testValidateRootVolumeType() {
    checkRootVolumeType("standard");
    verifyClean();

    checkRootVolumeType("gp2");
    verifyClean();

    checkRootVolumeType("random");
    verifySingleError(ROOT_VOLUME_TYPE);
  }

  @Test
  public void testValidateKeyName() {
    DescribeKeyPairsResult dkpResult = mock(DescribeKeyPairsResult.class);
    KeyPairInfo keypair = mock(KeyPairInfo.class);
    when(ec2Client.describeKeyPairs(any(DescribeKeyPairsRequest.class)))
        .thenReturn(dkpResult);
    when(dkpResult.getKeyPairs()).thenReturn(Collections.singletonList(keypair));

    String keyName = "keypair";
    checkKeyName(keyName);
    verifyClean();

    ArgumentCaptor<DescribeKeyPairsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeKeyPairsRequest.class);
    verify(ec2Client).describeKeyPairs(reqCaptor.capture());
    DescribeKeyPairsRequest req = reqCaptor.getValue();
    assertThat(req.getKeyNames()).isEqualTo(Lists.newArrayList(keyName));
  }

  @Test
  public void testvalidateKeyName_NoKeyPair() {
    DescribeKeyPairsResult dkpResult = mock(DescribeKeyPairsResult.class);
    when(ec2Client.describeKeyPairs(any(DescribeKeyPairsRequest.class)))
        .thenReturn(dkpResult);
    when(dkpResult.getKeyPairs()).thenReturn(Collections.<KeyPairInfo>emptyList());

    checkKeyName("keypair");
    verifySingleError(KEY_NAME);
  }

  @Test
  public void testvalidateKeyName_DuplicateKeyPair() {
    DescribeKeyPairsResult dkpResult = mock(DescribeKeyPairsResult.class);
    KeyPairInfo keypair = mock(KeyPairInfo.class);
    when(ec2Client.describeKeyPairs(any(DescribeKeyPairsRequest.class)))
        .thenReturn(dkpResult);
    when(dkpResult.getKeyPairs()).thenReturn(Lists.newArrayList(keypair, keypair));

    checkKeyName("keypair");
    verifySingleError(KEY_NAME);
  }

  @Test
  public void testValidateSpotParameters() {
    String[] validBlockDurations = {null, "", "60", "120", "180", "240", "300", "360"};

    checkSpotParameters(null, null, null);
    verifyClean();

    checkSpotParameters(null, "", null);
    verifyClean();

    checkSpotParameters(Boolean.FALSE, null, null);
    verifyClean();

    checkSpotParameters(Boolean.FALSE, "", null);
    verifyClean();

    for (String blockDuration : validBlockDurations) {
      checkSpotParameters(Boolean.TRUE, "0.031", blockDuration);
      verifyClean();
    }
  }

  @Test
  public void testValidateSpotParameters_MissingSpotBid() {
    checkSpotParameters(Boolean.TRUE, null, null);
    verifySingleError(SPOT_BID_USD_PER_HR);
  }

  @Test
  public void testValidateSpotParameters_EmptySpotBid() {
    checkSpotParameters(Boolean.TRUE, "", null);
    verifySingleError(SPOT_BID_USD_PER_HR);
  }

  @Test
  public void testValidateSpotParameters_MalformedSpotBid() {
    checkSpotParameters(Boolean.FALSE, "abc", null);
    verifySingleError(SPOT_BID_USD_PER_HR);
  }

  @Test
  public void testValidateSpotParameters_NegativeSpotBid() {
    checkSpotParameters(Boolean.TRUE, "-0.031", null);
    verifySingleError(SPOT_BID_USD_PER_HR);
  }

  @Test
  public void testValidateSpotParameters_InvalidBlockDuration() {
    String[] invalidBlockDurations = {
        "59", "61",
        "119", "121",
        "179", "181",
        "239", "241",
        "299", "301",
        "359", "361"
    };

    for (String blockDuration : invalidBlockDurations) {
      checkSpotParameters(Boolean.TRUE, "0.031", blockDuration);
      verifySingleError(BLOCK_DURATION_MINUTES);
      resetAccumulator();
    }
  }

  @Test
  public void testValidateEbsVolume() {
    String volumeType = "st1";
    String volumeCount = "2";
    String volumeSize = "800";
    String enableEncryption = "true";

    int minAllowedSize = 500;
    int maxAllowedSize = 16384;
    EbsVolumeMetadata ebsVolumeMetadata = new EbsVolumeMetadata(volumeType, minAllowedSize, maxAllowedSize);
    when(ebsMetadata.apply(volumeType)).thenReturn(ebsVolumeMetadata);

    checkEbsVolume(volumeType, volumeCount, volumeSize, enableEncryption, Optional.<String>absent());
    verifyClean();
  }

  @Test
  public void testValidateEbsVolumeCount_NotNumber() {
    String volumeCount = "not a number";
    checkEbsVolume("st1", volumeCount, "500", "false", Optional.<String>absent());
    verifySingleError(EBS_VOLUME_COUNT, INVALID_EBS_VOLUME_COUNT_FORMAT_MSG, volumeCount);
  }

  @Test
  public void testValidateEbsVolumeCount_Negative() {
    String volumeCount = "-1";
    checkEbsVolume("st1", volumeCount, "500", "false", Optional.<String>absent());
    verifySingleError(EBS_VOLUME_COUNT, INVALID_EBS_VOLUME_COUNT_MSG, MAX_VOLUMES_PER_INSTANCE);
  }

  @Test
  public void testValidateEbsVolumeCount_AboveMax() {
    String volumeCount = Integer.toString(MAX_VOLUMES_PER_INSTANCE + 1);
    checkEbsVolume("st1", volumeCount, "500", "false", Optional.<String>absent());
    verifySingleError(EBS_VOLUME_COUNT, INVALID_EBS_VOLUME_COUNT_MSG, MAX_VOLUMES_PER_INSTANCE);
  }

  @Test
  public void testValidateEbsVolumeSize_NotNumber() {
    String volumeType = "st1";
    String volumeSize = "not a number";
    checkEbsVolume(volumeType, "2", volumeSize, "false", Optional.<String>absent());
    verifySingleError(EBS_VOLUME_SIZE_GIB, INVALID_EBS_VOLUME_SIZE_FORMAT_MSG, volumeSize);
  }

  @Test
  public void testValidateEbsVolumeSize_BelowMin() {
    String volumeType = "st1";

    int minAllowedSize = 500;
    int maxAllowedSize = 16384;
    EbsVolumeMetadata ebsVolumeMetadata = new EbsVolumeMetadata(volumeType, minAllowedSize, maxAllowedSize);
    when(ebsMetadata.apply(volumeType)).thenReturn(ebsVolumeMetadata);

    String volumeSize = Integer.toString(minAllowedSize - 1);
    checkEbsVolume(volumeType, "2", volumeSize, "false", Optional.<String>absent());
    verifySingleError(EBS_VOLUME_SIZE_GIB, VOLUME_SIZE_NOT_IN_RANGE_MSG, volumeType, minAllowedSize, maxAllowedSize);
  }

  @Test
  public void testValidateEbsVolumeSize_AboveMax() {
    String volumeType = "st1";

    int minAllowedSize = 500;
    int maxAllowedSize = 16384;
    EbsVolumeMetadata ebsVolumeMetadata = new EbsVolumeMetadata(volumeType, minAllowedSize, maxAllowedSize);
    when(ebsMetadata.apply(volumeType)).thenReturn(ebsVolumeMetadata);

    String volumeSize = Integer.toString(maxAllowedSize + 1);
    checkEbsVolume(volumeType, "2", volumeSize, "false", Optional.<String>absent());
    verifySingleError(EBS_VOLUME_SIZE_GIB, VOLUME_SIZE_NOT_IN_RANGE_MSG, volumeType, minAllowedSize, maxAllowedSize);
  }

  @Test
  public void testValidateEbsEncryptionFlag_InvalidCount() {
    checkEbsVolume("st1", "0", "500", "true", Optional.<String>absent());
    verifySingleError(ENCRYPT_ADDITIONAL_EBS_VOLUMES, INVALID_EBS_ENCRYPTION_MSG);
  }

  @Test
  public void testValidateEbsKms_InvalidCount() {
    checkEbsVolume("st1", "0", "500", "false", Optional.of("some-kms-key"));
    verifySingleError(EBS_KMS_KEY_ID, INVALID_EBS_ENCRYPTION_MSG);
  }

  @Test
  public void testValidateEbsKms_KeyNotFound() {
    EbsVolumeMetadata ebsVolumeMetadata = new EbsVolumeMetadata("st1", 1, 1000);
    when(ebsMetadata.apply(anyString())).thenReturn(ebsVolumeMetadata);

    when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenThrow(new NotFoundException(""));

    checkEbsVolume("st1", "1", "500", "true", Optional.of("invalid-key"));
    verifySingleError(EBS_KMS_KEY_ID, INVALID_KMS_NOT_FOUND_MESSAGE);
  }

  protected void checkEbsVolume(String ebsVolumeType, String ebsVolumeCount,
                                String ebsVolumeSizeGib, String enableEncryption, Optional<String> ebsKmsKeyId) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(EBS_VOLUME_TYPE.unwrap().getConfigKey(), ebsVolumeType);
    configMap.put(EBS_VOLUME_COUNT.unwrap().getConfigKey(), ebsVolumeCount);
    configMap.put(EBS_VOLUME_SIZE_GIB.unwrap().getConfigKey(), ebsVolumeSizeGib);
    configMap.put(ENCRYPT_ADDITIONAL_EBS_VOLUMES.unwrap().getConfigKey(), enableEncryption);

    if (ebsKmsKeyId.isPresent()) {
      configMap.put(EBS_KMS_KEY_ID.unwrap().getConfigKey(), ebsKmsKeyId.get());
    }

    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkEbsVolumes(kmsClient, configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkImage with the specified configuration.
   *
   * @param amiName          the image name
   * @param type             the instance type
   * @param useSpotInstances whether to use Spot instances
   */
  protected void checkImage(String amiName, String type, boolean useSpotInstances) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(IMAGE.unwrap().getConfigKey(), amiName);
    configMap.put(TYPE.unwrap().getConfigKey(), type);
    if (useSpotInstances) {
      configMap.put(USE_SPOT_INSTANCES.unwrap().getConfigKey(), String.valueOf(true));
    }
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkImage(ec2Client, configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkAvailabilityZone with the specified configuration.
   *
   * @param zoneName the availability zone name
   */
  protected void checkAvailabilityZone(String zoneName) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(AVAILABILITY_ZONE.unwrap().getConfigKey(), zoneName);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkAvailabilityZone(ec2Client, configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkPlacementGroup with the specified configuration.
   *
   * @param placementGroup the placement group
   */
  protected void checkPlacementGroup(String placementGroup) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(PLACEMENT_GROUP.unwrap().getConfigKey(), placementGroup);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkPlacementGroup(ec2Client, configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkTenancy with the specified configuration.
   *
   * @param tenancy tenancy for the instance
   */
  protected void checkTenancy(String tenancy) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(TENANCY.unwrap().getConfigKey(), tenancy);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkTenancy(configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkIamProfileName with the specified configuration.
   *
   * @param iamProfileName the IAM profile name
   */
  protected void checkIamProfileName(String iamProfileName) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(IAM_PROFILE_NAME.unwrap().getConfigKey(), iamProfileName);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkIamProfileName(configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkKeyName with the specified configuration.
   *
   * @param keyName the key name
   */
  protected void checkKeyName(String keyName) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(KEY_NAME.unwrap().getConfigKey(), keyName);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkKeyName(ec2Client, configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkSecurityGroupIds with the specified configuration.
   *
   * @param securityGroupIds the security group IDs
   */
  protected void checkSecurityGroupIds(Collection<String> securityGroupIds) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(SECURITY_GROUP_IDS.unwrap().getConfigKey(),
        Joiner.on(',').join(securityGroupIds));
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkSecurityGroupIds(ec2Client, configuration, accumulator, localizationContext);
  }

  private void checkVpc(Map<String, String> vpcSubnetMap,
                        Map<String, Set<String>> vpcSecurityGroupMap) {
    validator.checkVpc(vpcSubnetMap, vpcSecurityGroupMap, accumulator, localizationContext);
  }

  /**
   * Invokes checkRootVolumeSize with the specified configuration.
   *
   * @param rootVolumeSize the root volume size
   */
  protected void checkRootVolumeSize(int rootVolumeSize) {
    checkRootVolumeSize(String.valueOf(rootVolumeSize));
  }

  /**
   * Invokes checkRootVolumeSize with the specified configuration.
   *
   * @param rootVolumeSizeString the root volume size string
   */
  protected void checkRootVolumeSize(String rootVolumeSizeString) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(ROOT_VOLUME_SIZE_GB.unwrap().getConfigKey(), rootVolumeSizeString);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkRootVolumeSize(configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkRootVolumeType with the specified configuration.
   *
   * @param rootVolumeType the root volume type
   */
  protected void checkRootVolumeType(String rootVolumeType) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(ROOT_VOLUME_TYPE.unwrap().getConfigKey(), rootVolumeType);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkRootVolumeType(configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkSubnetId with the specified configuration.
   *
   * @param subnetId the subnet ID
   */
  protected void checkSubnetId(String subnetId) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(SUBNET_ID.unwrap().getConfigKey(), subnetId);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkSubnetId(ec2Client, configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkSpotParameters with the specified configuration.
   *
   * @param useSpotInstances     whether to use spot instances
   * @param spotBidUSDPerHr      the spot bid, in USD/hr
   * @param blockDurationMinutes the spot block duration, in minutes
   */
  protected void checkSpotParameters(Boolean useSpotInstances, String spotBidUSDPerHr,
      String blockDurationMinutes) {
    Map<String, String> configMap = Maps.newHashMap();
    if (useSpotInstances != null) {
      configMap.put(USE_SPOT_INSTANCES.unwrap().getConfigKey(), useSpotInstances.toString());
    }
    if (spotBidUSDPerHr != null) {
      configMap.put(SPOT_BID_USD_PER_HR.unwrap().getConfigKey(), spotBidUSDPerHr);
    }
    if (blockDurationMinutes != null) {
      configMap.put(BLOCK_DURATION_MINUTES.unwrap().getConfigKey(), blockDurationMinutes);
    }
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkSpotParameters(configuration, accumulator, localizationContext);
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains no errors or
   * warnings.
   */
  private void verifyClean() {
    Map<String, Collection<PluginExceptionCondition>> conditionsByKey =
        accumulator.getConditionsByKey();
    assertThat(conditionsByKey).isEmpty();
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains exactly
   * one condition, which must be an error associated with the specified property.
   *
   * @param token the configuration property token for the property which should be in error
   */
  private void verifySingleError(ConfigurationPropertyToken token) {
    verifySingleError(token, Optional.<String>absent());
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains exactly
   * one condition, which must be an error with the specified message and associated with the
   * specified property.
   *
   * @param token    the configuration property token for the property which should be in error
   * @param errorMsg the expected error message
   * @param args     the error message arguments
   */
  private void verifySingleError(ConfigurationPropertyToken token,
      String errorMsg, Object... args) {
    verifySingleError(token, Optional.of(errorMsg), args);
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains exactly
   * one condition, which must be an error with the specified message and associated with the
   * specified property.
   *
   * @param token          the configuration property token for the property which should be in error
   * @param errorMsgFormat the expected error message
   * @param args           the error message arguments
   */
  private void verifySingleError(ConfigurationPropertyToken token,
      Optional<String> errorMsgFormat, Object... args) {
    Map<String, Collection<PluginExceptionCondition>> conditionsByKey =
        accumulator.getConditionsByKey();
    assertThat(conditionsByKey).hasSize(1);
    String configKey = token.unwrap().getConfigKey();
    assertThat(conditionsByKey).containsKey(configKey);
    Collection<PluginExceptionCondition> keyConditions = conditionsByKey.get(configKey);
    assertThat(keyConditions).hasSize(1);
    PluginExceptionCondition condition = keyConditions.iterator().next();
    verifySingleErrorCondition(condition, errorMsgFormat, args);
  }

  /**
   * Verifies that the specified plugin exception condition is an error with the specified message.
   *
   * @param condition      the plugin exception condition
   * @param errorMsgFormat the expected error message format
   * @param args           the error message arguments
   */
  private void verifySingleErrorCondition(PluginExceptionCondition condition,
      Optional<String> errorMsgFormat, Object... args) {
    assertThat(condition.isError()).isTrue();
    if (errorMsgFormat.isPresent()) {
      assertThat(condition.getMessage()).isEqualTo(String.format(errorMsgFormat.get(), args));
    }
  }

  /**
   * Resets the accumulator.
   */
  private void resetAccumulator() {
    // Replace the accumulator because it doesn't support reset.
    accumulator = new PluginExceptionConditionAccumulator();
  }
}
