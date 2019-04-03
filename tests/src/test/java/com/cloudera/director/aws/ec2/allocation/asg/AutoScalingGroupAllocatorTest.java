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

package com.cloudera.director.aws.ec2.allocation.asg;

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.allocation.asg.AutoScalingGroupAllocator.AUTO_SCALING_GROUP_REQUEST_DURATION_MS;
import static com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException.ErrorType;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.AUTOMATIC;
import static com.cloudera.director.spi.v2.model.InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX;
import static com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate.SimpleResourceTemplateConfigurationPropertyToken.GROUP_ID;
import static com.cloudera.director.spi.v2.provider.Launcher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.cloudera.director.aws.ec2.allocation.AllocationHelper;
import com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.DetachInstancesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.DetachInstancesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.LaunchTemplateSpecification;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.CreateLaunchTemplateResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DeleteLaunchTemplateResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeLaunchTemplatesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeLaunchTemplatesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.LaunchTemplate;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClient;
import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.exception.AbstractPluginException;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v2.model.exception.TransientProviderException;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.assertj.core.util.Maps;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests {@link AutoScalingGroupAllocator}.
 */
public class AutoScalingGroupAllocatorTest {

  private static void putConfig(Map<String, String> configMap, ConfigurationPropertyToken propertyToken,
      String value) {
    if (value != null) {
      configMap.put(propertyToken.unwrap().getConfigKey(), value);
    }
  }

  private AllocationHelper allocationHelper;
  private AmazonEC2AsyncClient ec2Client;
  private AmazonAutoScalingAsyncClient autoScalingClient;
  private AWSSecurityTokenServiceAsyncClient stsClient;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    Map<String, Object> configMap = Maps.newHashMap();
    // Set a short timeout so retries will fail quickly.
    configMap.put(AUTO_SCALING_GROUP_REQUEST_DURATION_MS, 2000L);
    Config config = ConfigFactory.parseMap(configMap);
    AWSTimeouts awsTimeouts = new AWSTimeouts(config);
    EC2TagHelper ec2TagHelper = mock(EC2TagHelper.class);

    allocationHelper = mock(AllocationHelper.class);
    when(allocationHelper.getAWSTimeouts()).thenReturn(awsTimeouts);
    when(allocationHelper.getEC2TagHelper()).thenReturn(ec2TagHelper);

    ec2Client = mock(AmazonEC2AsyncClient.class);
    autoScalingClient = mock(AmazonAutoScalingAsyncClient.class);
    stsClient = mock(AWSSecurityTokenServiceAsyncClient.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate() throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeLaunchTemplatesSuccess();
    mockCreateLaunchTemplateSuccess();
    mockDescribeAutoScalingGroupsSuccess();
    mockCreateAutoScalingGroupSuccess();

    Collection<EC2Instance> instances = autoScalingGroupAllocator.allocate();
    assertThat(instances).isNotNull();
    assertThat(instances).isEmpty();

    verify(ec2Client, times(1)).createLaunchTemplate(any());
    verify(autoScalingClient, times(1)).createAutoScalingGroup(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocateNone() throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDescribeLaunchTemplatesSuccess();
    mockCreateLaunchTemplateSuccess();
    mockDescribeAutoScalingGroupsSuccess();
    mockCreateAutoScalingGroupSuccess();

    Collection<EC2Instance> instances = autoScalingGroupAllocator.allocate();
    assertThat(instances).isNotNull();
    assertThat(instances).isEmpty();

//    verify(ec2Client, times(1))
//        .describeLaunchTemplates(any(DescribeLaunchTemplatesRequest.class));
    verify(ec2Client, times(1)).createLaunchTemplate(any());
//    verify(autoScalingClient, times(1))
//        .describeAutoScalingGroups(any(DescribeAutoScalingGroupsRequest.class));
    verify(autoScalingClient, times(1)).createAutoScalingGroup(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_UnrecoverableExceptionCreatingLaunchTemplate()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDescribeLaunchTemplatesSuccess();

    // Mock launch template creation with AWS exception
    when(ec2Client.createLaunchTemplate(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.allocate();

//    verify(ec2Client, times(1))
//        .describeLaunchTemplates(any(DescribeLaunchTemplatesRequest.class));
    verify(ec2Client, times(1)).createLaunchTemplate(any());
//    verify(autoScalingClient, times(0))
//        .describeAutoScalingGroups(any(DescribeAutoScalingGroupsRequest.class));
    verify(autoScalingClient, times(1)).createAutoScalingGroup(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_TransientExceptionCreatingLaunchTemplate()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDescribeLaunchTemplatesSuccess();

    // Mock launch template creation with AWS exception
    when(ec2Client.createLaunchTemplate(any()))
        .thenThrow(newRetryableAmazonServiceException());

    mockDeleteLaunchTemplateSuccess();

    expectedException.expect(
        nestedMatcher(TransientProviderException.class, "OtherRetryableError"));

    autoScalingGroupAllocator.allocate();

//    verify(ec2Client, times(1))
//        .describeLaunchTemplates(any(DescribeLaunchTemplatesRequest.class));
    verify(ec2Client, times(1)).createLaunchTemplate(any());
//    verify(autoScalingClient, times(0))
//        .describeAutoScalingGroups(any(DescribeAutoScalingGroupsRequest.class));
    verify(autoScalingClient, times(0)).createAutoScalingGroup(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_UnrecoverableExceptionCreatingASG()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDescribeLaunchTemplatesSuccess();
    mockCreateLaunchTemplateSuccess();
    mockDescribeAutoScalingGroupsSuccess();

    // Mock ASG creation with AWS exception
    when(autoScalingClient.createAutoScalingGroup(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    mockDeleteLaunchTemplateSuccess();

    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.allocate();

//    verify(ec2Client, times(1))
//        .describeLaunchTemplates(any(DescribeLaunchTemplatesRequest.class));
    verify(ec2Client, times(1)).createLaunchTemplate(any());
//    verify(autoScalingClient, times(1))
//        .describeAutoScalingGroups(any(DescribeAutoScalingGroupsRequest.class));
    verify(autoScalingClient, times(1)).createAutoScalingGroup(any());
    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_TransientExceptionCreatingASG()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDescribeLaunchTemplatesSuccess();
    mockCreateLaunchTemplateSuccess();
    mockDescribeAutoScalingGroupsSuccess();

    // Mock ASG creation with AWS exception
    when(autoScalingClient.createAutoScalingGroup(any()))
        .thenThrow(newRetryableAmazonServiceException());

    mockDeleteLaunchTemplateSuccess();
    mockDeleteAutoScalingGroupSuccess();

    expectedException.expect(
        nestedMatcher(TransientProviderException.class, "OtherRetryableError"));

    autoScalingGroupAllocator.allocate();

//    verify(ec2Client, times(1))
//        .describeLaunchTemplates(any(DescribeLaunchTemplatesRequest.class));
    verify(ec2Client, times(1)).createLaunchTemplate(any());
//    verify(autoScalingClient, times(1))
//        .describeAutoScalingGroups(any(DescribeAutoScalingGroupsRequest.class));
    verify(autoScalingClient, times(1)).createAutoScalingGroup(any());
    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_SympatheticFailureCreatingASG()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDescribeLaunchTemplatesSuccess();
    mockCreateLaunchTemplateSuccess();
    mockDescribeAutoScalingGroupsSuccess();

    // Mock ASG creation with AWS exception
    when(autoScalingClient.createAutoScalingGroup(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    // Mock launch template cleanup with AWS exception
    when(ec2Client.deleteLaunchTemplate(any()))
        .thenThrow(newRetryableAmazonServiceException());

    // Expect that the exception captures the ASG creation failure, and not just the sympathetic
    // launch template cleanup failure
    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.allocate();

//    verify(ec2Client, times(1))
//        .describeLaunchTemplates(any(DescribeLaunchTemplatesRequest.class));
    verify(ec2Client, times(1)).createLaunchTemplate(any());
//    verify(autoScalingClient, times(1))
//        .describeAutoScalingGroups(any(DescribeAutoScalingGroupsRequest.class));
    verify(autoScalingClient, times(1)).createAutoScalingGroup(any());
    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_TransientExceptionDeletingLaunchTemplate()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    // Mock launch template deletion with AWS exception
    when(ec2Client.deleteLaunchTemplate(any()))
        .thenThrow(newRetryableAmazonServiceException());

    mockDeleteAutoScalingGroupSuccess();

    expectedException.expect(
        nestedMatcher(TransientProviderException.class, "OtherRetryableError"));

    autoScalingGroupAllocator.delete();

    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_UnrecoverableExceptionDeletingASG()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDeleteLaunchTemplateSuccess();

    // Mock ASG deletion with AWS exception
    when(autoScalingClient.deleteAutoScalingGroup(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    mockDeleteLaunchTemplateSuccess();

    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.delete();

    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_TransientExceptionDeletingASG()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    // Mock ASG deletion with AWS exception
    when(autoScalingClient.deleteAutoScalingGroup(any()))
        .thenThrow(newRetryableAmazonServiceException());

    mockDeleteLaunchTemplateSuccess();

    expectedException.expect(
        nestedMatcher(TransientProviderException.class, "OtherRetryableError"));

    autoScalingGroupAllocator.delete();

    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAllocate_SympatheticFailureDeletingASG()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    // Mock ASG deletion with AWS exception
    when(autoScalingClient.deleteAutoScalingGroup(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    // Mock launch template cleanup with AWS exception
    when(ec2Client.deleteLaunchTemplate(any()))
        .thenThrow(newRetryableAmazonServiceException());

    // Expect that the exception captures the ASG deletion failure, and not just the sympathetic
    // launch template cleanup failure
    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.delete();

    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteGroup() throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    mockDeleteLaunchTemplateSuccess();
    mockDeleteAutoScalingGroupSuccess();

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(0)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(0)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteGroup_UnrecoverableExceptionDeletingLaunchTemplate()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 0, 0);

    // Mock launch template deletion with AWS exception
    when(ec2Client.deleteLaunchTemplate(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    mockDeleteAutoScalingGroupSuccess();

    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(0)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(0)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(1)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(1)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances() throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeAutoScalingGroupsSuccess();
    mockDetachInstancesSuccess();

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(1)).detachInstances(any());
    verify(allocationHelper, times(1)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_ReduceMinCount()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeAutoScalingGroupsSuccess(10, 3, groupId);
    mockUpdateAutoScalingGroupSuccess();
    mockDetachInstancesSuccess();

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(1)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(1)).detachInstances(any());
    verify(allocationHelper, times(1)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_SomeInstancesNotInAutoScalingGroup()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeAutoScalingGroupsSuccess();

    // Mock Auto Scaling group description with AWS exception
    when(autoScalingClient.detachInstances(any()))
        .thenThrow(newAmazonServiceException(
            "The instance foo is not part of Auto Scaling group " + groupId + ".",
            ErrorType.Client, "ValidationError"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(11)).detachInstances(any());
    verify(allocationHelper, times(1)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_UnrecoverableExceptionDescribingAutoScalingGroup()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    // Mock Auto Scaling group description with AWS exception
    when(autoScalingClient.describeAutoScalingGroups(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(0)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_TransientExceptionDescribingAutoScalingGroup()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    // Mock Auto Scaling group description with AWS exception
    when(autoScalingClient.describeAutoScalingGroups(any()))
        .thenThrow(newRetryableAmazonServiceException());

    expectedException.expect(
        nestedMatcher(TransientProviderException.class, "OtherRetryableError"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(0)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_UnrecoverableExceptionUpdatingAutoScalingGroup()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeAutoScalingGroupsSuccess(10, 3, groupId);

    // Mock Auto Scaling group update with AWS exception
    when(autoScalingClient.updateAutoScalingGroup(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(1)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(0)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_TransientExceptionUpdatingAutoScalingGroup()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeAutoScalingGroupsSuccess(10, 3, groupId);

    // Mock Auto Scaling group update with AWS exception
    when(autoScalingClient.updateAutoScalingGroup(any()))
        .thenThrow(newRetryableAmazonServiceException());

    expectedException.expect(
        nestedMatcher(TransientProviderException.class, "OtherRetryableError"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(1)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(0)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_UnrecoverableExceptionDetachingAllInstances()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeAutoScalingGroupsSuccess();

    // Mock Auto Scaling group description with AWS exception
    when(autoScalingClient.detachInstances(any()))
        .thenThrow(newUnrecoverableAmazonServiceException());

    expectedException.expect(
        nestedMatcher(UnrecoverableProviderException.class, "InvalidParameterValue"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(1)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteInstances_TransientExceptionDetachingAllInstances()
      throws InterruptedException, ExecutionException, TimeoutException {
    String groupId = UUID.randomUUID().toString();
    AutoScalingGroupAllocator autoScalingGroupAllocator =
        createAutoScalingGroupAllocator(groupId, 10, 0);

    mockDescribeAutoScalingGroupsSuccess();

    // Mock Auto Scaling group description with AWS exception
    when(autoScalingClient.detachInstances(any()))
        .thenThrow(newRetryableAmazonServiceException());

    expectedException.expect(
        nestedMatcher(TransientProviderException.class, "OtherRetryableError"));

    autoScalingGroupAllocator.delete();

    verify(autoScalingClient, times(1)).describeAutoScalingGroups(any());
    verify(autoScalingClient, times(0)).updateAutoScalingGroup(any());
    verify(autoScalingClient, times(1)).detachInstances(any());
    verify(allocationHelper, times(0)).doDelete(any());
    verify(ec2Client, times(0)).deleteLaunchTemplate(any());
    verify(autoScalingClient, times(0)).deleteAutoScalingGroup(any());
  }

  private AutoScalingGroupAllocator createAutoScalingGroupAllocator(String groupId,
      int desiredCount, int minCount) {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    for (int i = 0; i < desiredCount; i++) {
      listBuilder.add(UUID.randomUUID().toString());
    }
    return createAutoScalingGroupAllocator(groupId, listBuilder.build(), minCount);
  }

  private AutoScalingGroupAllocator createAutoScalingGroupAllocator(String groupId,
      Collection<String> instanceIds, int minCount) {
    return new AutoScalingGroupAllocator(allocationHelper, ec2Client, autoScalingClient, stsClient,
        createEC2InstanceTemplate(groupId), instanceIds, minCount);
  }

  private EC2InstanceTemplate createEC2InstanceTemplate(String groupId) {
    Map<String, String> instanceTemplateConfigMap = new LinkedHashMap<>();
    String templateName = "test-template";
    putConfig(instanceTemplateConfigMap, INSTANCE_NAME_PREFIX, templateName);
    putConfig(instanceTemplateConfigMap, IMAGE, "ami-test");
    putConfig(instanceTemplateConfigMap, SECURITY_GROUP_IDS, "sg-test");
    putConfig(instanceTemplateConfigMap, SUBNET_ID, "sb-test");
    putConfig(instanceTemplateConfigMap, TYPE, "m3.medium");
    putConfig(instanceTemplateConfigMap, AUTOMATIC, "true");
    putConfig(instanceTemplateConfigMap, GROUP_ID, groupId);

    Map<String, String> instanceTemplateTags = new LinkedHashMap<>();
    instanceTemplateTags.put(Tags.InstanceTags.OWNER.getTagKey(), "test-user");

    return new EC2InstanceTemplate(
        templateName, new SimpleConfiguration(instanceTemplateConfigMap), instanceTemplateTags,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  private void mockDescribeLaunchTemplatesSuccess(String... launchTemplateNames) {
    // Mock launch templates
    List<LaunchTemplate> launchTemplates =
        mockLaunchTemplates(launchTemplateNames);
    // Mock launch template description
    DescribeLaunchTemplatesResult launchTemplatesResult =
        mock(DescribeLaunchTemplatesResult.class);
    when(launchTemplatesResult.getLaunchTemplates()).thenReturn(launchTemplates);
    when(ec2Client.describeLaunchTemplates(
        any(DescribeLaunchTemplatesRequest.class)))
        .thenReturn(launchTemplatesResult);
  }

  private void mockCreateLaunchTemplateSuccess() {
    // Mock launch template creation
    CreateLaunchTemplateResult launchTemplateResult =
        mock(CreateLaunchTemplateResult.class);
    when(ec2Client.createLaunchTemplate(any()))
        .thenReturn(launchTemplateResult);
  }

  private void mockDescribeAutoScalingGroupsSuccess(String... asgNames) {
    mockDescribeAutoScalingGroupsSuccess(null, null, asgNames);
  }

  private void mockDescribeAutoScalingGroupsSuccess(Integer desiredCapacity, Integer minSize,
      String... asgNames) {
    // Mock Auto Scaling Groups
    List<AutoScalingGroup> autoScalingGroups =
        mockAutoScalingGroups(desiredCapacity, minSize, asgNames);
    // Mock Auto Scaling Group description
    DescribeAutoScalingGroupsResult autoScalingGroupsResult =
        mock(DescribeAutoScalingGroupsResult.class);
    when(autoScalingGroupsResult.getAutoScalingGroups()).thenReturn(autoScalingGroups);
    when(autoScalingClient.describeAutoScalingGroups(
        any(DescribeAutoScalingGroupsRequest.class)))
        .thenReturn(autoScalingGroupsResult);
  }

  private void mockCreateAutoScalingGroupSuccess() {
    // Mock ASG creation
    CreateAutoScalingGroupResult asgResult =
        mock(CreateAutoScalingGroupResult.class);
    when(autoScalingClient.createAutoScalingGroup(any()))
        .thenReturn(asgResult);
  }

  private void mockDeleteLaunchTemplateSuccess() {
    // Mock launch template deletion
    DeleteLaunchTemplateResult launchTemplateResult =
        mock(DeleteLaunchTemplateResult.class);
    when(ec2Client.deleteLaunchTemplate(any()))
        .thenReturn(launchTemplateResult);
  }

  private void mockDeleteAutoScalingGroupSuccess() {
    // Mock ASG deletion
    DeleteAutoScalingGroupResult asgResult =
        mock(DeleteAutoScalingGroupResult.class);
    when(autoScalingClient.deleteAutoScalingGroup(any()))
        .thenReturn(asgResult);
  }

  private void mockUpdateAutoScalingGroupSuccess() {
    // Mock ASG update
    UpdateAutoScalingGroupResult asgResult =
        mock(UpdateAutoScalingGroupResult.class);
    when(autoScalingClient.updateAutoScalingGroup(any()))
        .thenReturn(asgResult);
  }

  private void mockDetachInstancesSuccess() {
    // Mock instance detachment
    DetachInstancesResult detachInstancesResult =
        mock(DetachInstancesResult.class);
    when(autoScalingClient.detachInstances(any()))
        .thenReturn(detachInstancesResult);
  }

  private List<LaunchTemplate> mockLaunchTemplates(String[] launchTemplateNames) {
    ImmutableList.Builder<LaunchTemplate> builder =
        ImmutableList.<LaunchTemplate>builder();
    for (String launchTemplateName : launchTemplateNames) {
      LaunchTemplate launchTemplate = mock(LaunchTemplate.class);
      when(launchTemplate.getLaunchTemplateName()).thenReturn(launchTemplateName);
      builder.add(launchTemplate);
    }

    return builder.build();
  }

  private List<AutoScalingGroup> mockAutoScalingGroups(Integer desiredCapacity, Integer minSize,
      String[] asgNames) {
    ImmutableList.Builder<AutoScalingGroup> builder =
        ImmutableList.<AutoScalingGroup>builder();
    for (String asgName : asgNames) {
      AutoScalingGroup autoScalingGroup = mock(AutoScalingGroup.class);
      when(autoScalingGroup.getAutoScalingGroupName()).thenReturn(asgName);
      LaunchTemplateSpecification launchTemplateSpecification = mock(LaunchTemplateSpecification.class);
      when(launchTemplateSpecification.getLaunchTemplateName()).thenReturn(asgName);
      when(autoScalingGroup.getLaunchTemplate()).thenReturn(launchTemplateSpecification);
      if (desiredCapacity != null) {
        when(autoScalingGroup.getDesiredCapacity()).thenReturn(desiredCapacity);
      }
      if (minSize != null) {
        when(autoScalingGroup.getMinSize()).thenReturn(minSize);
      }
      builder.add(autoScalingGroup);
    }

    return builder.build();
  }

  private static AmazonServiceException newUnrecoverableAmazonServiceException() {
    return newAmazonServiceException("Unrecoverable", ErrorType.Client, "InvalidParameterValue");
  }

  private static AmazonServiceException newRetryableAmazonServiceException() {
    return newAmazonServiceException("Retryable", ErrorType.Service, "OtherRetryableError");
  }

  private static AmazonServiceException newAmazonServiceException(String errorMessage,
      ErrorType errorType, String errorCode) {
    AmazonServiceException e = new AmazonServiceException(errorMessage);
    e.setErrorType(errorType);
    e.setErrorCode(errorCode);
    return e;
  }

  /**
   * Returns a matcher that matches a Throwable of the specified type when the specified message
   * substring is contained in the message of the Throwable itself, any of its nested causes,
   * any of its nested suppressed exceptions, or any of its nested plugin exception conditions.
   *
   * @param outerThrowableType the expected outer throwable type
   * @param messageSubstring   the message substring to match
   * @return a matcher that matches a Throwable of the specified type when the specified message
   * substring is contained in the message of the Throwable itself, any of its nested causes,
   * any of its nested suppressed exceptions, or any of its nested plugin exception conditions
   */
  private TypeSafeMatcher<Throwable> nestedMatcher(
      Class<? extends Throwable> outerThrowableType, String messageSubstring) {
    return new TypeSafeMatcher<Throwable>() {
      @Override
      public void describeTo(Description description) {
      }

      @Override
      protected boolean matchesSafely(Throwable throwable) {
        return (outerThrowableType.isInstance(throwable))
            && hasNestedMatch(throwable);
      }

      private boolean hasNestedMatch(Throwable throwable) {
        while (throwable != null) {
          if (throwable.getMessage().contains(messageSubstring)) {
            return true;
          }
          if (hasMatchingSuppressed(throwable)) {
            return true;
          }
          if ((throwable instanceof AbstractPluginException)
              && hasMatchingDetails((AbstractPluginException) throwable)) {
            return true;
          }
          throwable = throwable.getCause();
        }
        return false;
      }

      private boolean hasMatchingSuppressed(Throwable throwable) {
        for (Throwable t : throwable.getSuppressed()) {
          if (t.getMessage().contains(messageSubstring)) {
            return true;
          }
        }
        return false;
      }

      private boolean hasMatchingDetails(AbstractPluginException e) {
        PluginExceptionDetails details = e.getDetails();
        for (SortedSet<PluginExceptionCondition> conditions : details.getConditionsByKey().values()) {
          for (PluginExceptionCondition condition : conditions) {
            for (String value : condition.getExceptionInfo().values())
            if (value.contains(messageSubstring)) {
              return true;
            }
          }
        }
        return false;
      }
    };
  }
}
