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

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.AVAILABILITY_ZONE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IAM_PROFILE_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.PLACEMENT_GROUP;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_SIZE_GB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.HVM_VIRTUALIZATION;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_ARCHITECTURE_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_NAME_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_PLATFORM_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_AMI_ROOT_DEVICE_TYPE_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.INVALID_IAM_PROFILE_NAME_MSG;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.MIN_ROOT_VOLUME_SIZE_GB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.PARAVIRTUAL_VIRTUALIZATION;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplateConfigurationValidator.ROOT_VOLUME_TYPES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
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
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.PlacementGroup;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.VolumeType;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
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

@SuppressWarnings("PMD.TooManyStaticImports")
public class EC2InstanceTemplateConfigurationValidatorTest {

  private static final String IMAGE_NAME = "ami-1234567";
  private static final InstanceType VALID_INSTANCE_TYPE = InstanceType.T2Micro;
  private static final String TYPE_STRING = VALID_INSTANCE_TYPE.toString();

  private EC2Provider ec2Provider;
  private VirtualizationMappings virtualizationMappings;
  private EC2InstanceTemplateConfigurationValidator validator;
  private AmazonEC2Client ec2Client;
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  @Before
  public void setUp() {
    virtualizationMappings = mock(VirtualizationMappings.class);
    ec2Client = mock(AmazonEC2Client.class);
    ec2Provider = mock(EC2Provider.class);
    when(ec2Provider.getClient()).thenReturn(ec2Client);
    when(ec2Provider.getVirtualizationMappings()).thenReturn(virtualizationMappings);
    validator = new EC2InstanceTemplateConfigurationValidator(ec2Provider);
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

  private Image mockSingleCheckedImage() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    Image image = mock(Image.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Collections.singletonList(image));
    return image;
  }

  private void mockImageAttributes(Image image, String architecture, String platform,
      String state, String virtualizationType, String rootDeviceType) {
    when(image.getArchitecture()).thenReturn(architecture);
    when(image.getPlatform()).thenReturn(platform);
    when(image.getState()).thenReturn(state);
    when(image.getVirtualizationType()).thenReturn(virtualizationType);
    when(image.getRootDeviceType()).thenReturn(rootDeviceType);
  }

  @Test
  public void testCheckImage() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "paravirtual", "ebs");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING);
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
    checkImage(amiName, TYPE_STRING);
    verifySingleError(IMAGE, INVALID_AMI_NAME_MSG, amiName);
  }

  @Test
  public void testCheckImage_NoAMI() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Collections.<Image>emptyList());

    checkImage(IMAGE_NAME, TYPE_STRING);
    verifySingleError(IMAGE);
  }

  @Test
  public void testCheckImage_DuplicateAMI() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    Image image = mock(Image.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Lists.newArrayList(image, image));

    checkImage(IMAGE_NAME, TYPE_STRING);
    verifySingleError(IMAGE);
  }

  @Test
  public void testCheckImage_Not64Bit() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86", null, "available", "paravirtual", "ebs");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING);
    verifySingleError(IMAGE, INVALID_AMI_ARCHITECTURE_MSG, IMAGE_NAME, "x86");
  }

  @Test
  public void testCheckImage_Windows() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", "Windows", "available", "paravirtual", "ebs");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING);
    verifySingleError(IMAGE, INVALID_AMI_PLATFORM_MSG, IMAGE_NAME, "Windows");
  }

  @Test
  public void testCheckImage_InvalidState() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "newjersey", "paravirtual", "ebs");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING);
    verifySingleError(IMAGE);
  }

  @Test
  public void testCheckImage_IncompatibleInstanceType() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "hvm", "ebs");
    when(virtualizationMappings.apply(HVM_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING + "x"));
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING);
    verifySingleError(IMAGE, INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY_MSG, TYPE_STRING, "hvm",
        IMAGE_NAME);
  }

  @Test
  public void testCheckImage_RootDeviceType() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "paravirtual", "instance-store");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    checkImage(IMAGE_NAME, TYPE_STRING);
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

  /**
   * Invokes checkImage with the specified configuration.
   *
   * @param amiName the image name
   * @param type    the instance type
   */
  protected void checkImage(String amiName, String type) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(IMAGE.unwrap().getConfigKey(), amiName);
    configMap.put(TYPE.unwrap().getConfigKey(), type);
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
   * Invokes checkIamProfileName with the specified configuration.
   *
   * @param iamProfileName the IAM profile name
   */
  protected void checkIamProfileName(String iamProfileName) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(IAM_PROFILE_NAME.unwrap().getConfigKey(), iamProfileName);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkIamProfileName(ec2Provider, configuration, accumulator, localizationContext);
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
    assertThat(conditionsByKey.containsKey(configKey)).isTrue();
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
}
