// (c) Copyright 2017 Cloudera, Inc.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.kms.AWSKMSClient;
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.aws.common.AWSKMSClientProvider;
import com.cloudera.director.aws.common.AmazonEC2ClientProvider;
import com.cloudera.director.aws.common.AmazonIdentityManagementClientProvider;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken;
import com.cloudera.director.aws.ec2.ebs.EBSDeviceMappings;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class EC2ProviderTests {

  private static final LocalizationContext LOCALIZATION_CONTEXT =
      new DefaultLocalizationContext(Locale.getDefault(), "");
  private static final Map<String, String> TEMPLATE_CONFIGS = ImmutableMap.<String, String>builder()
      .put(EC2InstanceTemplateConfigurationPropertyToken.TYPE.unwrap().getConfigKey(), "m4.xlarge")
      .put(EC2InstanceTemplateConfigurationPropertyToken.IMAGE.unwrap().getConfigKey(), "ami-12345678")
      .put(EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID.unwrap().getConfigKey(), "ami-12345678")
      .put(EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS.unwrap().getConfigKey(), "sg-12345678")
      .build();
  private static final EC2InstanceTemplate TEST_TEMPLATE = new EC2InstanceTemplate("instance-template",
      new SimpleConfiguration(TEMPLATE_CONFIGS), ImmutableMap.<String, String>of(), LOCALIZATION_CONTEXT);

  private AmazonEC2AsyncClient ec2Client;
  private AmazonIdentityManagementClient identityClient;
  private AWSKMSClient kmsClient;

  @Test
  public void testAllocate() throws ExecutionException, InterruptedException {
    EC2Provider ec2Provider = getTestEc2Provider();

    when(ec2Client.describeInstances(any(DescribeInstancesRequest.class)))
        .thenReturn(makeDescribeInstancesResult(
            makeInstance("1i", "1v", InstanceStateName.Running, "192.0.2.1"),
            makeInstance("2i", "2v", InstanceStateName.Running, "192.0.2.2"),
            makeInstance("3i", "3v", InstanceStateName.Running, "192.0.2.3")));
    when(ec2Client.describeInstanceAttribute(any(DescribeInstanceAttributeRequest.class)))
        .thenThrow(new AmazonServiceException("dummyException"));

    when(ec2Client.describeImages(any(DescribeImagesRequest.class)))
        .thenReturn(makeDescribeImagesResult());

    when(ec2Client.runInstancesAsync(any(RunInstancesRequest.class))).then(new Answer<Future<RunInstancesResult>>() {
      @Override
      public Future<RunInstancesResult> answer(InvocationOnMock invocation) throws Throwable {

        List<Tag> allTags =
            tagSpecificationsToTags(((RunInstancesRequest) invocation.getArguments()[0]).getTagSpecifications());

        if (tagsContainsVirtualInstanceId(allTags, "4v")) {
          return makeRunInstancesResult("4i", "4v", InstanceStateName.Running, "192.0.2.4");
        } else if (tagsContainsVirtualInstanceId(allTags, "5v")) {
          return makeRunInstancesResult("5i", "5v", InstanceStateName.Running, "192.0.2.5");
        }

        return null;
      }
    });

    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("1i")))
        .thenReturn(makeDescribeInstanceStatusResult("1i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("2i")))
        .thenReturn(makeDescribeInstanceStatusResult("2i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("3i")))
        .thenReturn(makeDescribeInstanceStatusResult("3i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("4i")))
        .thenReturn(makeDescribeInstanceStatusResult("4i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("5i")))
        .thenReturn(makeDescribeInstanceStatusResult("5i", InstanceStateName.Running));

    assertThat(ec2Provider.allocate(TEST_TEMPLATE, Lists.newArrayList("1v", "2v", "3v", "4v", "5v"), 1)).hasSize(5);
  }

  @Test
  public void testAllocateSomeTerminateWhileWaitingForIp() throws ExecutionException, InterruptedException {
    EC2Provider ec2Provider = getTestEc2Provider();

    when(ec2Client.describeInstances(any(DescribeInstancesRequest.class)))
        .thenAnswer(new Answer<DescribeInstancesResult>() {
          @Override
          public DescribeInstancesResult answer(InvocationOnMock invocation) throws Throwable {
            DescribeInstancesRequest request = (DescribeInstancesRequest) invocation.getArguments()[0];

            if (!request.getFilters().isEmpty()) {
              return makeDescribeInstancesResult(
                  makeInstance("1i", "1v", InstanceStateName.Running, "192.0.2.1"),
                  makeInstance("2i", "2v", InstanceStateName.Running, "192.0.2.2"),
                  makeInstance("3i", "3v", InstanceStateName.Running, "192.0.2.3"));
            } else if (!request.getInstanceIds().isEmpty()) {
              return makeDescribeInstancesResult(
                  makeInstance("5i", "5v", InstanceStateName.Terminated, null),
                  makeInstance("6i", "6v", InstanceStateName.Terminated, null));
            }

            return null;
          }
        });

    when(ec2Client.describeInstanceAttribute(any(DescribeInstanceAttributeRequest.class)))
        .thenThrow(new AmazonServiceException("dummyException"));

    when(ec2Client.describeImages(any(DescribeImagesRequest.class)))
        .thenReturn(makeDescribeImagesResult());

    when(ec2Client.runInstancesAsync(any(RunInstancesRequest.class))).then(new Answer<Future<RunInstancesResult>>() {
      @Override
      public Future<RunInstancesResult> answer(InvocationOnMock invocation) throws Throwable {

        List<Tag> allTags =
            tagSpecificationsToTags(((RunInstancesRequest) invocation.getArguments()[0]).getTagSpecifications());

        if (tagsContainsVirtualInstanceId(allTags, "4v")) {
          return makeRunInstancesResult("4i", "4v", InstanceStateName.Running, "192.0.2.4");
        } else if (tagsContainsVirtualInstanceId(allTags, "5v")) {
          return makeRunInstancesResult("5i", "5v", InstanceStateName.Running, null);
        } else if (tagsContainsVirtualInstanceId(allTags, "6v")) {
          return makeRunInstancesResult("6i", "6v", InstanceStateName.Running, null);
        }

        return null;
      }
    });

    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("1i")))
        .thenReturn(makeDescribeInstanceStatusResult("1i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("2i")))
        .thenReturn(makeDescribeInstanceStatusResult("2i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("3i")))
        .thenReturn(makeDescribeInstanceStatusResult("3i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("4i")))
        .thenReturn(makeDescribeInstanceStatusResult("4i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("5i")))
        .thenReturn(makeDescribeInstanceStatusResult("5i", InstanceStateName.Running));
    when(ec2Client.describeInstanceStatus(makeDescribeInstanceStatusRequest("6i")))
        .thenReturn(makeDescribeInstanceStatusResult("6i", InstanceStateName.Running));

    assertThat(ec2Provider.allocate(TEST_TEMPLATE, Lists.newArrayList("1v", "2v", "3v", "4v", "5v", "6v"), 1))
        .hasSize(4);
  }

  private EC2Provider getTestEc2Provider() {
    return getTestEc2Provider(ImmutableMap.<String, String>of());
  }

  private EC2Provider getTestEc2Provider(Map<String, String> tagMappings) {

    EphemeralDeviceMappings ephemeralDeviceMappings = new EphemeralDeviceMappings(new SimpleConfiguration(),
        new File("dummy"), LOCALIZATION_CONTEXT);
    EBSDeviceMappings ebsDeviceMappings = mock(EBSDeviceMappings.class);
    EBSMetadata ebsMetadata = new EBSMetadata(new SimpleConfiguration(),
        new File("dummy"), LOCALIZATION_CONTEXT);
    VirtualizationMappings virtualizationMappings = new VirtualizationMappings(new SimpleConfiguration(),
        new File("dummy"), LOCALIZATION_CONTEXT);
    AWSFilters awsFilters = new AWSFilters(Optional.<Config>absent());
    AWSTimeouts awsTimeouts = new AWSTimeouts(null);
    CustomTagMappings customTagMappings = new CustomTagMappings(ConfigFactory.parseMap(tagMappings));
    NetworkRules networkRules = NetworkRules.EMPTY_RULES;
    AmazonEC2ClientProvider ec2ClientProvider = mock(AmazonEC2ClientProvider.class);
    ec2Client = mock(AmazonEC2AsyncClient.class);
    when(ec2ClientProvider.getClient(any(Configured.class), any(PluginExceptionConditionAccumulator.class),
        any(LocalizationContext.class), anyBoolean())).thenReturn(ec2Client);
    AmazonIdentityManagementClientProvider identityClientProvider = mock(AmazonIdentityManagementClientProvider.class);
    identityClient = mock(AmazonIdentityManagementClient.class);
    when(identityClientProvider.getClient(any(Configured.class), any(PluginExceptionConditionAccumulator.class),
        any(LocalizationContext.class), anyBoolean())).thenReturn(identityClient);
    AWSKMSClientProvider kmsClientProvider = mock(AWSKMSClientProvider.class);
    kmsClient = mock(AWSKMSClient.class);
    when(kmsClientProvider.getClient(any(Configured.class), any(PluginExceptionConditionAccumulator.class),
        any(LocalizationContext.class), anyBoolean())).thenReturn(kmsClient);

    return new EC2Provider(new SimpleConfiguration(), ephemeralDeviceMappings, ebsDeviceMappings, ebsMetadata,
        virtualizationMappings, awsFilters, awsTimeouts, customTagMappings, networkRules,
        ec2ClientProvider, identityClientProvider, kmsClientProvider, true, LOCALIZATION_CONTEXT);
  }

  private DescribeInstancesResult makeDescribeInstancesResult(Instance... instances) {
    List<Reservation> reservations = Lists.newArrayList();

    for (Instance instance : instances) {
      reservations.add(new Reservation().withInstances(instance));
    }

    return new DescribeInstancesResult().withReservations(reservations);
  }

  private Instance makeInstance(String instanceId, String virtualInstanceId, InstanceStateName stateName,
      String ipAddress) {
    return makeInstance(instanceId, Lists.newArrayList(
        new Tag().withKey(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey()).withValue(virtualInstanceId)), stateName,
        ipAddress);
  }

  private Instance makeInstance(String instanceId, List<Tag> tags, InstanceStateName stateName, String ipAddress) {
    return new Instance()
        .withImageId("ami-12345678")
        .withInstanceId(instanceId).withTags(tags)
        .withState(new InstanceState().withName(stateName))
        .withPrivateIpAddress(ipAddress);
  }

  private DescribeInstanceStatusRequest makeDescribeInstanceStatusRequest(String instanceId) {
    return new DescribeInstanceStatusRequest()
        .withIncludeAllInstances(true)
        .withInstanceIds(instanceId);
  }

  private DescribeInstanceStatusResult makeDescribeInstanceStatusResult(String instanceId,
      InstanceStateName stateName) {
    return new DescribeInstanceStatusResult()
        .withInstanceStatuses(makeInstanceStatus(instanceId, stateName));
  }

  private InstanceStatus makeInstanceStatus(String instanceId, InstanceStateName stateName) {
    return new InstanceStatus().withInstanceId(instanceId).withInstanceState(new InstanceState().withName(stateName));
  }

  private DescribeImagesResult makeDescribeImagesResult() {
    return new DescribeImagesResult()
        .withImages(new Image()
            .withRootDeviceType(EC2Provider.DEVICE_TYPE_EBS)
            .withRootDeviceName("/dev/sda1")
            .withBlockDeviceMappings(new BlockDeviceMapping()
                .withDeviceName("/dev/sda1")
                .withEbs(new EbsBlockDevice())));
  }

  private Future<RunInstancesResult> makeRunInstancesResult(String instanceId, String virtualInstanceId,
      InstanceStateName stateName, String ipAddress) throws ExecutionException, InterruptedException {
    Future<RunInstancesResult> mockFuture = mock(Future.class);

    when(mockFuture.get()).thenReturn(new RunInstancesResult()
        .withReservation(new Reservation()
            .withReservationId(instanceId)
            .withInstances(makeInstance(instanceId, virtualInstanceId, stateName, ipAddress))));

    return mockFuture;
  }

  private List<Tag> tagSpecificationsToTags(List<TagSpecification> tagSpecifications) {
    List<Tag> allTags = Lists.newArrayList();
    for (TagSpecification tagSpecification : tagSpecifications) {
      allTags.addAll(tagSpecification.getTags());
    }
    return allTags;
  }

  private boolean tagsContainsVirtualInstanceId(List<Tag> tags, String virtualInstanceId) {
    return tags.contains(new Tag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), virtualInstanceId));
  }
}
