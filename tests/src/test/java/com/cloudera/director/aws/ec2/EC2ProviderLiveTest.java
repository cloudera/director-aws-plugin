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

import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_COUNT;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_SIZE_GIB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SYSTEM_DISKS;
import static com.cloudera.director.aws.test.EC2ProviderCommon.CENTOS67_HVM_AMI_BY_REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.waitUntilRunningOrTerminal;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.getEc2Provider;
import static com.cloudera.director.aws.test.EC2ProviderCommon.isLive;
import static com.cloudera.director.aws.test.EC2ProviderCommon.putConfig;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.Tags.InstanceTags;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Volume;
import com.cloudera.director.aws.test.LiveTestProperties;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Filter;
import com.cloudera.director.aws.test.TestInstanceTemplate;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.exception.AbstractPluginException;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;

import org.assertj.core.util.Lists;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests {@link EC2Provider}.
 */
public class EC2ProviderLiveTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static LiveTestProperties liveTestProperties;

  @BeforeClass
  public static void checkIsLive() {
    assumeTrue(isLive());
    liveTestProperties = LiveTestProperties.loadLiveTestProperties();
  }

  private AWSCredentialsProvider createCredentialsProvider() {
    Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
    SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
    return new AWSCredentialsProviderChainProvider()
        .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  private TestInstanceTemplate newTemplate() {
    String region = liveTestProperties.getTestRegion();
    String securityGroup = liveTestProperties.getTestSecurityGroup();
    String subnet = liveTestProperties.getTestSubnet();

    String image = CENTOS67_HVM_AMI_BY_REGION.get(region).getId();
    TestInstanceTemplate template = new TestInstanceTemplate();
    template.addConfig(IMAGE, image);
    template.addConfig(SECURITY_GROUP_IDS, securityGroup);
    template.addConfig(SUBNET_ID, subnet);

    return template;
  }

  /**
   * Will fail the test if there are running instances of the specified type.
   *
   * @param ec2Provider an EC2Provider
   * @param instanceType the instance type to look for
   */
  private void assertNoRunningInstances(EC2Provider ec2Provider, String instanceType) {
    DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
    describeInstancesRequest.withFilters(
        new Filter("instance-type").withValues(instanceType),
        new Filter("instance-state-name").withValues("running")
    );
    AmazonEC2Client client = ec2Provider.getClient();
    DescribeInstancesResult result = client.describeInstances(describeInstancesRequest);

    if (result.getReservations().size() != 0) {
      String message = String.format("Unexpected state, there are already running instance(s) for " +
          "instance type %s", instanceType);
      fail(message);
    }
  }

  @Test
  public void testCreateEC2InstanceWithExcessiveTags() throws InterruptedException {
    AWSCredentialsProvider credentialsProvider = createCredentialsProvider();

    // Configure provider
    LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
    putConfig(providerConfigMap, REGION, "us-east-1");

    // Create provider
    EC2Provider ec2Provider = getEc2Provider(new SimpleConfiguration(providerConfigMap),
        credentialsProvider);
    // Create tags map with exceeding tags size limit
    Map<String, String> instanceTemplateTags = Maps.newHashMap();
    String username = System.getProperty("user.name");
    instanceTemplateTags.put(InstanceTags.OWNER.getTagKey(), username);
    for (int i = 0; i <= EC2Provider.MAX_TAGS_ALLOWED + 1; i++) {
      instanceTemplateTags.put("key" + i, "someval");
    }
    EC2InstanceTemplate template = mock(EC2InstanceTemplate.class);
    when(template.getTags()).thenReturn(instanceTemplateTags);
    thrown.expect(UnrecoverableProviderException.class);
    Collection<String> requestId = ImmutableList.of("virtualId");
    try {
      ec2Provider.allocate(template, requestId, 1);
    } finally {
      ec2Provider.delete(template, requestId);
    }

  }

  @Test
  public void testVolumeLimits() throws InterruptedException {

    // This test will exercise volume limit behavior in the EC2Provider.

    LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
    putConfig(providerConfigMap, REGION, liveTestProperties.getTestRegion());

    EC2Provider ec2Provider = getEc2Provider(new SimpleConfiguration(providerConfigMap), createCredentialsProvider());

    TestInstanceTemplate template = newTemplate();
    template.addConfig(TYPE, "m3.medium");

    template.addConfig(EBS_VOLUME_COUNT, "25");
    template.addConfig(EBS_VOLUME_SIZE_GIB, "16000");
    template.addConfig(EBS_VOLUME_TYPE, "st1");

    EC2InstanceTemplate instanceTemplate = ec2Provider.createResourceTemplate(
        template.getTemplateName(), new SimpleConfiguration(template.getConfigs()),
        template.getTags()
    );

    List<String> requestId = Lists.newArrayList(UUID.randomUUID().toString());
    try {
      ec2Provider.allocate(instanceTemplate, requestId, 1);
    } catch (UnrecoverableProviderException ex) {
      verifySingleErrorCode(ex, AWSExceptions.VOLUME_LIMIT_EXCEEDED);
    } finally {
      ec2Provider.delete(instanceTemplate, requestId);
    }
  }

  @Test
  public void testInstanceLimits() throws InterruptedException {

    // This test will exercise instance limit behavior in the EC2Provider.

    String region = liveTestProperties.getTestRegion();
    LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
    putConfig(providerConfigMap, REGION, region);

    EC2Provider ec2Provider = getEc2Provider(new SimpleConfiguration(providerConfigMap), createCredentialsProvider());

    TestInstanceTemplate template = newTemplate();

    // We use i3.4xlarge instance type which should have a limit of 2 running instances.
    // Note that this test will fail if the AWS account has a higher limit for this instance
    // type or if there are instances of this type already running.

    String instanceType = "i3.4xlarge";
    template.addConfig(TYPE, instanceType);

    // make sure there aren't any running to begin with
    assertNoRunningInstances(ec2Provider, instanceType);

    EC2InstanceTemplate instanceTemplate = ec2Provider.createResourceTemplate(
        template.getTemplateName(), new SimpleConfiguration(template.getConfigs()),
        template.getTags()
    );

    // We make 2 separate requests both of which attempt to allocate 2 instances.
    // We expect the first request to succeed and the second request to fail.

    List<String> requestIds1 = Arrays.asList(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    );
    List<String> requestIds2 = Arrays.asList(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    );

    try {
      int requestCount1 = requestIds1.size();
      int requestCount2 = requestIds2.size();

      // First request should succeed

      Collection<EC2Instance> instances = ec2Provider.allocate(instanceTemplate, requestIds1, requestCount1);
      assertTrue(requestCount1 == instances.size());
      Map<String, InstanceState> result = waitUntilRunningOrTerminal(ec2Provider, instanceTemplate, requestIds1);
      for (InstanceState state : result.values()) {
        assertTrue(state.getInstanceStatus().equals(InstanceStatus.RUNNING));
      }

      // Second request should not succeed and should throw an exception with the
      // appropriate error information.

      ec2Provider.allocate(instanceTemplate, requestIds2, requestCount2);
      fail("Expected an UnrecoverableProviderException to be thrown");

    } catch(UnrecoverableProviderException ex) {
      verifySingleErrorCode(ex, AWSExceptions.INSTANCE_LIMIT_EXCEEDED);
    } finally {
      ec2Provider.delete(instanceTemplate, requestIds1);
      ec2Provider.delete(instanceTemplate, requestIds2);
    }
  }

  @Test
  public void testSystemDisk() throws InterruptedException {
    LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
    putConfig(providerConfigMap, REGION, liveTestProperties.getTestRegion());

    EC2Provider ec2Provider = getEc2Provider(new SimpleConfiguration(providerConfigMap), createCredentialsProvider());

    TestInstanceTemplate template = newTemplate();
    template.addConfig(TYPE, "m3.medium");

    int templateDiskSize = 50;
    template.addConfig(EBS_VOLUME_COUNT, "2");
    template.addConfig(EBS_VOLUME_SIZE_GIB, Integer.toString(templateDiskSize));
    template.addConfig(EBS_VOLUME_TYPE, "gp2");

    int systemDiskSize01 = 85;
    int systemDiskSize02 = 90;

    String systemDisksConfig = "[ \n" +
        "  { \"volumeType\": \"gp2\", \"volumeSize\": \"%d\", \"enableEncryption\": \"false\" },\n" +
        "  { \"volumeType\": \"gp2\", \"volumeSize\": \"%d\", \"enableEncryption\": \"false\" } \n" +
        "]";

    template.addConfig(
        SYSTEM_DISKS,
        String.format(systemDisksConfig, systemDiskSize01, systemDiskSize02)
    );

    EC2InstanceTemplate instanceTemplate = ec2Provider.createResourceTemplate(
        template.getTemplateName(), new SimpleConfiguration(template.getConfigs()),
        template.getTags()
    );

    List<String> requestId = Lists.newArrayList(UUID.randomUUID().toString());
    try {
      ec2Provider.allocate(instanceTemplate, requestId, 1);

      // verify there are 5 volumes (1 root volume, 2 volumes from template, 2 system disk volumes)
      List<Volume> volumes = ec2Provider.getVolumes(Iterables.getOnlyElement(requestId));
      assertTrue(volumes.size() == 5);

      boolean hasTemplateDisk = false;
      boolean hasSystemDisk01 = false;
      boolean hasSystemDisk02 = false;

      for (Volume volume : volumes) {
        int volumeSize = volume.getSize();
        hasTemplateDisk = hasTemplateDisk || (volumeSize == templateDiskSize);
        hasSystemDisk01 = hasSystemDisk01 || (volumeSize == systemDiskSize01);
        hasSystemDisk02 = hasSystemDisk02 || (volumeSize == systemDiskSize02);
      }

      assertTrue(hasTemplateDisk);
      assertTrue(hasSystemDisk01);
      assertTrue(hasSystemDisk02);
    } finally {
      ec2Provider.delete(instanceTemplate, requestId);
    }
  }


  private void verifySingleErrorCode(AbstractPluginException ex, String errorCode) {
    Map<String, SortedSet<PluginExceptionCondition>> conditionsByKey =
        ex.getDetails().getConditionsByKey();
    SortedSet<PluginExceptionCondition> conditions = conditionsByKey.get(null);
    PluginExceptionCondition condition = Iterables.getOnlyElement(conditions);
    Map<String, String> exceptionInfo = condition.getExceptionInfo();
    assertTrue(exceptionInfo.get("awsErrorCode").equals(errorCode));
  }
}
