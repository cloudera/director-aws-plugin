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

import static com.cloudera.director.ec2.EC2Validations.HVM_VIRTUALIZATION;
import static com.cloudera.director.ec2.EC2Validations.PARAVIRTUAL_VIRTUALIZATION;
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
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EC2ValidationsTest {

  private static final String IMAGE_NAME = "ami-1234567";
  private static final InstanceType VALID_INSTANCE_TYPE = InstanceType.T2Micro;
  private static final String TYPE_STRING = VALID_INSTANCE_TYPE.toString();

  private VirtualizationMappings virtualizationMappings;
  private EC2Validations validations;
  private AmazonEC2Client ec2Client;
  private List<String> errors;

  @Before
  public void setUp() {
    virtualizationMappings = mock(VirtualizationMappings.class);
    validations = new EC2Validations(virtualizationMappings);
    ec2Client = mock(AmazonEC2Client.class);
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

    assertThat(types).containsAll(EC2Validations.ROOT_VOLUME_TYPES);
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

    assertThat(validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING)).isEmpty();

    ArgumentCaptor<DescribeImagesRequest> reqCaptor = ArgumentCaptor.forClass(DescribeImagesRequest.class);
    verify(ec2Client).describeImages(reqCaptor.capture());
    DescribeImagesRequest req = reqCaptor.getValue();
    assertThat(req.getImageIds()).isEqualTo(Lists.newArrayList(IMAGE_NAME));
  }

  @Test
  public void testCheckImage_NonAMI() {
    errors = validations.checkImage(ec2Client, "xyz-1234567", TYPE_STRING);
    assertThat(errors).hasSize(1);
    assertThat(errors).contains(EC2Validations.INVALID_AMI_NAME);
  }

  @Test
  public void testCheckImage_NoAMI() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Collections.<Image>emptyList());

    errors = validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING);
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testCheckImage_DuplicateAMI() {
    DescribeImagesResult diResult = mock(DescribeImagesResult.class);
    Image image = mock(Image.class);
    when(ec2Client.describeImages(any(DescribeImagesRequest.class))).thenReturn(diResult);
    when(diResult.getImages()).thenReturn(Lists.newArrayList(image, image));

    errors = validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING);
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testCheckImage_Not64Bit() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86", null, "available", "paravirtual", "ebs");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    errors = validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING);
    assertThat(errors).hasSize(1);
    assertThat(errors).contains(String.format(EC2Validations.INVALID_AMI_ARCHITECTURE, IMAGE_NAME, "x86"));
  }

  @Test
  public void testCheckImage_Windows() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", "Windows", "available", "paravirtual", "ebs");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    errors = validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING);
    assertThat(errors).hasSize(1);
    assertThat(errors).contains(String.format(EC2Validations.INVALID_AMI_PLATFORM, IMAGE_NAME, "Windows"));
  }

  @Test
  public void testCheckImage_InvalidState() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "newjersey", "paravirtual", "ebs");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    errors = validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING);
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testCheckImage_IncompatibleInstanceType() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "hvm", "ebs");
    when(virtualizationMappings.apply(HVM_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING + "x"));
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    errors = validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING);
    assertThat(errors).hasSize(1);
    assertThat(errors).contains(String.format(EC2Validations.INVALID_AMI_INSTANCE_TYPE_COMPATIBILITY,
        TYPE_STRING, "hvm", IMAGE_NAME));
  }

  @Test
  public void testCheckImage_RootDeviceType() {
    Image image = mockSingleCheckedImage();
    mockImageAttributes(image, "x86_64", null, "available", "paravirtual", "instance-store");
    when(virtualizationMappings.apply(PARAVIRTUAL_VIRTUALIZATION))
        .thenReturn(ImmutableList.of(TYPE_STRING));

    errors = validations.checkImage(ec2Client, IMAGE_NAME, TYPE_STRING);
    assertThat(errors).hasSize(1);
    assertThat(errors).contains(String.format(EC2Validations.INVALID_AMI_ROOT_DEVICE_TYPE, IMAGE_NAME, "instance-store"));
  }

  @Test
  public void testCheckAvailabilityZone() {
    DescribeAvailabilityZonesResult dazResult = mock(DescribeAvailabilityZonesResult.class);
    AvailabilityZone zone = mock(AvailabilityZone.class);
    when(ec2Client.describeAvailabilityZones(any(DescribeAvailabilityZonesRequest.class)))
        .thenReturn(dazResult);
    when(dazResult.getAvailabilityZones()).thenReturn(Collections.singletonList(zone));

    assertThat(validations.checkAvailabilityZone(ec2Client, "zone")).isEmpty();

    ArgumentCaptor<DescribeAvailabilityZonesRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeAvailabilityZonesRequest.class);
    verify(ec2Client).describeAvailabilityZones(reqCaptor.capture());
    DescribeAvailabilityZonesRequest req = reqCaptor.getValue();
    assertThat(req.getZoneNames()).isEqualTo(Lists.newArrayList("zone"));
  }

  @Test
  public void testCheckAvailabilityZone_NoZone() {
    DescribeAvailabilityZonesResult dazResult = mock(DescribeAvailabilityZonesResult.class);
    when(ec2Client.describeAvailabilityZones(any(DescribeAvailabilityZonesRequest.class)))
        .thenReturn(dazResult);
    when(dazResult.getAvailabilityZones()).thenReturn(Collections.<AvailabilityZone>emptyList());

    errors = validations.checkAvailabilityZone(ec2Client, "zone");
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testCheckAvailabilityZone_DuplicateZone() {
    DescribeAvailabilityZonesResult dazResult = mock(DescribeAvailabilityZonesResult.class);
    AvailabilityZone zone = mock(AvailabilityZone.class);
    when(ec2Client.describeAvailabilityZones(any(DescribeAvailabilityZonesRequest.class)))
        .thenReturn(dazResult);
    when(dazResult.getAvailabilityZones()).thenReturn(Lists.newArrayList(zone, zone));

    errors = validations.checkAvailabilityZone(ec2Client, "zone");
    // error message is private
  }

  @Test
  public void testCheckPlacementGroup() {
    DescribePlacementGroupsResult dpgResult = mock(DescribePlacementGroupsResult.class);
    PlacementGroup group = mock(PlacementGroup.class);
    when(ec2Client.describePlacementGroups(any(DescribePlacementGroupsRequest.class)))
        .thenReturn(dpgResult);
    when(dpgResult.getPlacementGroups()).thenReturn(Collections.singletonList(group));

    assertThat(validations.checkPlacementGroup(ec2Client, "group")).isEmpty();

    ArgumentCaptor<DescribePlacementGroupsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribePlacementGroupsRequest.class);
    verify(ec2Client).describePlacementGroups(reqCaptor.capture());
    DescribePlacementGroupsRequest req = reqCaptor.getValue();
    assertThat(req.getGroupNames()).isEqualTo(Lists.newArrayList("group"));
  }

  @Test
  public void testCheckPlacementGroup_NoGroup() {
    DescribePlacementGroupsResult dpgResult = mock(DescribePlacementGroupsResult.class);
    when(ec2Client.describePlacementGroups(any(DescribePlacementGroupsRequest.class)))
        .thenReturn(dpgResult);
    when(dpgResult.getPlacementGroups()).thenReturn(Collections.<PlacementGroup>emptyList());

    errors = validations.checkPlacementGroup(ec2Client, "group");
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testCheckPlacementGroup_DuplicateGroup() {
    DescribePlacementGroupsResult dpgResult = mock(DescribePlacementGroupsResult.class);
    PlacementGroup group = mock(PlacementGroup.class);
    when(ec2Client.describePlacementGroups(any(DescribePlacementGroupsRequest.class)))
        .thenReturn(dpgResult);
    when(dpgResult.getPlacementGroups()).thenReturn(Lists.newArrayList(group, group));

    errors = validations.checkPlacementGroup(ec2Client, "group");
    // error message is private
  }

  @Test
  public void testCheckIamProfileName() {
    AmazonIdentityManagement iamClient = mock(AmazonIdentityManagement.class);
    GetInstanceProfileResult gipResult = mock(GetInstanceProfileResult.class);
    when(iamClient.getInstanceProfile(any(GetInstanceProfileRequest.class)))
        .thenReturn(gipResult);

    assertThat(validations.checkIamProfileName(iamClient, "role")).isEmpty();

    ArgumentCaptor<GetInstanceProfileRequest> reqCaptor =
        ArgumentCaptor.forClass(GetInstanceProfileRequest.class);
    verify(iamClient).getInstanceProfile(reqCaptor.capture());
    GetInstanceProfileRequest req = reqCaptor.getValue();
    assertThat(req.getInstanceProfileName()).isEqualTo("role");
  }

  @Test
  public void testCheckIamProfileName_NoSuchEntity() {
    AmazonIdentityManagement iamClient = mock(AmazonIdentityManagement.class);
    when(iamClient.getInstanceProfile(any(GetInstanceProfileRequest.class)))
        .thenThrow(new NoSuchEntityException("role"));

    errors = validations.checkIamProfileName(iamClient, "role");
    assertThat(errors).hasSize(1);
    assertThat(errors).contains(String.format(EC2Validations.INVALID_IAM_PROFILE_NAME, "role"));
  }

  @Test
  public void testCheckSubnetId() {
    DescribeSubnetsResult dsResult = mock(DescribeSubnetsResult.class);
    Subnet subnet = mock(Subnet.class);
    when(ec2Client.describeSubnets(any(DescribeSubnetsRequest.class)))
        .thenReturn(dsResult);
    when(dsResult.getSubnets()).thenReturn(Collections.singletonList(subnet));

    assertThat(validations.checkSubnetId(ec2Client, "subnet")).isEmpty();

    ArgumentCaptor<DescribeSubnetsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeSubnetsRequest.class);
    verify(ec2Client).describeSubnets(reqCaptor.capture());
    DescribeSubnetsRequest req = reqCaptor.getValue();
    assertThat(req.getSubnetIds()).isEqualTo(Lists.newArrayList("subnet"));
  }

  @Test
  public void testCheckSubnetId_NoSubnet() {
    DescribeSubnetsResult dsResult = mock(DescribeSubnetsResult.class);
    when(ec2Client.describeSubnets(any(DescribeSubnetsRequest.class)))
        .thenReturn(dsResult);
    when(dsResult.getSubnets()).thenReturn(Collections.<Subnet>emptyList());

    errors = validations.checkSubnetId(ec2Client, "subnet");
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testCheckSubnetId_DuplicateSubnet() {
    DescribeSubnetsResult dsResult = mock(DescribeSubnetsResult.class);
    Subnet subnet = mock(Subnet.class);
    when(ec2Client.describeSubnets(any(DescribeSubnetsRequest.class)))
        .thenReturn(dsResult);
    when(dsResult.getSubnets()).thenReturn(Lists.newArrayList(subnet, subnet));

    errors = validations.checkSubnetId(ec2Client, "subnet");
    // error message is private
  }

  @Test
  public void testCheckSecurityGroupsIds() {
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    SecurityGroup securityGroup = mock(SecurityGroup.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Collections.singletonList(securityGroup));

    assertThat(validations.checkSecurityGroupsIds(ec2Client, Collections.singletonList("securityGroup"))).isEmpty();

    ArgumentCaptor<DescribeSecurityGroupsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeSecurityGroupsRequest.class);
    verify(ec2Client).describeSecurityGroups(reqCaptor.capture());
    DescribeSecurityGroupsRequest req = reqCaptor.getValue();
    assertThat(req.getGroupIds()).isEqualTo(Lists.newArrayList("securityGroup"));
  }

  @Test
  public void testCheckSecurityGroupsIds_NoSecurityGroup() {
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Collections.<SecurityGroup>emptyList());

    errors = validations.checkSecurityGroupsIds(ec2Client, Collections.singletonList("securityGroup"));
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testCheckSecurityGroupsIds_DuplicateSecurityGroup() {
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    SecurityGroup securityGroup = mock(SecurityGroup.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Lists.newArrayList(securityGroup, securityGroup));

    errors = validations.checkSecurityGroupsIds(ec2Client, Collections.singletonList("securityGroup"));
    // error message is private
  }

  @Test
  public void testCheckRootVolumeSize() {
    assertThat(validations.checkRootVolumeSize(EC2Validations.MIN_ROOT_VOLUME_SIZE_GB)).isEmpty();

    errors = validations.checkRootVolumeSize(EC2Validations.MIN_ROOT_VOLUME_SIZE_GB - 1);
    assertThat(errors).hasSize(1);
  }

  @Test
  public void testValidateRootVolumeType() {
    assertThat(validations.checkRootVolumeType("standard")).isEmpty();
    assertThat(validations.checkRootVolumeType("gp2")).isEmpty();

    assertThat(validations.checkRootVolumeType("random")).hasSize(1);
  }

  @Test
  public void testValidateKeyName() {
    DescribeKeyPairsResult dkpResult = mock(DescribeKeyPairsResult.class);
    KeyPairInfo keypair = mock(KeyPairInfo.class);
    when(ec2Client.describeKeyPairs(any(DescribeKeyPairsRequest.class)))
        .thenReturn(dkpResult);
    when(dkpResult.getKeyPairs()).thenReturn(Collections.singletonList(keypair));

    assertThat(validations.validateKeyName(ec2Client, "keypair")).isEmpty();

    ArgumentCaptor<DescribeKeyPairsRequest> reqCaptor =
        ArgumentCaptor.forClass(DescribeKeyPairsRequest.class);
    verify(ec2Client).describeKeyPairs(reqCaptor.capture());
    DescribeKeyPairsRequest req = reqCaptor.getValue();
    assertThat(req.getKeyNames()).isEqualTo(Lists.newArrayList("keypair"));
  }

  @Test
  public void testvalidateKeyName_NoKeyPair() {
    DescribeKeyPairsResult dkpResult = mock(DescribeKeyPairsResult.class);
    when(ec2Client.describeKeyPairs(any(DescribeKeyPairsRequest.class)))
        .thenReturn(dkpResult);
    when(dkpResult.getKeyPairs()).thenReturn(Collections.<KeyPairInfo>emptyList());

    errors = validations.validateKeyName(ec2Client, "keypair");
    assertThat(errors).hasSize(1);
    // error message is private
  }

  @Test
  public void testvalidateKeyName_DuplicateKeyPair() {
    DescribeKeyPairsResult dkpResult = mock(DescribeKeyPairsResult.class);
    KeyPairInfo keypair = mock(KeyPairInfo.class);
    when(ec2Client.describeKeyPairs(any(DescribeKeyPairsRequest.class)))
        .thenReturn(dkpResult);
    when(dkpResult.getKeyPairs()).thenReturn(Lists.newArrayList(keypair, keypair));

    errors = validations.validateKeyName(ec2Client, "keypair");
    // error message is private
  }
}
