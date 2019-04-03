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

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SPOT_BID_USD_PER_HR;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;
import static com.cloudera.director.spi.v2.model.InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX;
import static com.cloudera.director.spi.v2.provider.Launcher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.cloudera.director.aws.ec2.allocation.AllocationHelper;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClient;
import com.cloudera.director.aws.shaded.org.joda.time.DateTime;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link SpotGroupAllocator}.
 */
public class SpotGroupAllocatorTest {

  private static void putConfig(Map<String, String> configMap, ConfigurationPropertyToken propertyToken,
      String value) {
    if (value != null) {
      configMap.put(propertyToken.unwrap().getConfigKey(), value);
    }
  }

  private AllocationHelper allocationHelper;
  private AmazonEC2AsyncClient ec2Client;
  private AWSSecurityTokenServiceAsyncClient stsClient;

  @Before
  public void setUp() {
    AWSTimeouts awsTimeouts = new AWSTimeouts(null);
    EC2TagHelper ec2TagHelper = mock(EC2TagHelper.class);

    allocationHelper = mock(AllocationHelper.class);
    when(allocationHelper.getAWSTimeouts()).thenReturn(awsTimeouts);
    when(allocationHelper.getEC2TagHelper()).thenReturn(ec2TagHelper);

    ec2Client = mock(AmazonEC2AsyncClient.class);
    stsClient = mock(AWSSecurityTokenServiceAsyncClient.class);
  }

  @Test(timeout=5000L)
  public void testSpotGroupAllocatorTagSpotInstancesSuccess() throws Exception {
    Set<String> virtualInstanceIds = ImmutableSet.of("vid1", "vid2");
    SpotGroupAllocator spotGroupAllocator = createSpotGroupAllocator(virtualInstanceIds);

    for (String virtualInstanceId : virtualInstanceIds) {
      SpotAllocationRecord record = spotGroupAllocator.getSpotAllocationRecord(virtualInstanceId);
      // set some value for the ec2InstanceId to pretend that the instance was allocated
      record.ec2InstanceId = virtualInstanceId;
    }

    when(allocationHelper.waitUntilInstanceHasStarted(any(), any())).thenReturn(true);

    spotGroupAllocator.tagSpotInstances(DateTime.now().plus(1000L));
    for (String virtualInstanceId : virtualInstanceIds) {
      SpotAllocationRecord record = spotGroupAllocator.getSpotAllocationRecord(virtualInstanceId);
      assertThat(record.instanceTagged).isTrue();
    }
  }

  @Test(timeout=5000L)
  public void testSpotGroupAllocatorTagSpotInstancesFailed() throws Exception {
    Set<String> virtualInstanceIds = ImmutableSet.of("vid1", "vid2");
    SpotGroupAllocator spotGroupAllocator = createSpotGroupAllocator(virtualInstanceIds);

    for (String virtualInstanceId : virtualInstanceIds) {
      SpotAllocationRecord record = spotGroupAllocator.getSpotAllocationRecord(virtualInstanceId);
      // set some value for the ec2InstanceId to pretend that the instance was allocated
      record.ec2InstanceId = virtualInstanceId;
    }

    when(allocationHelper.waitUntilInstanceHasStarted(any(), any())).thenReturn(false);

    spotGroupAllocator.tagSpotInstances(DateTime.now().plus(1000L));
    for (String virtualInstanceId : virtualInstanceIds) {
      SpotAllocationRecord record = spotGroupAllocator.getSpotAllocationRecord(virtualInstanceId);
      assertThat(record.instanceTagged).isFalse();
    }
  }

  @Test(timeout=20000L)
  public void testSpotGroupAllocatorTagSpotInstancesTimeout() throws Exception {
    Set<String> virtualInstanceIds = ImmutableSet.of("vid1", "vid2");
    SpotGroupAllocator spotGroupAllocator = createSpotGroupAllocator(virtualInstanceIds);

    for (String virtualInstanceId : virtualInstanceIds) {
      SpotAllocationRecord record = spotGroupAllocator.getSpotAllocationRecord(virtualInstanceId);
      // set some value for the ec2InstanceId to pretend that the instance was allocated
      record.ec2InstanceId = virtualInstanceId;
    }

    when(allocationHelper.waitUntilInstanceHasStarted(any(), any())).thenThrow(TimeoutException.class);

    spotGroupAllocator.tagSpotInstances(DateTime.now().plus(1000L));

    for (String virtualInstanceId : virtualInstanceIds) {
      SpotAllocationRecord record = spotGroupAllocator.getSpotAllocationRecord(virtualInstanceId);
      assertThat(record.instanceTagged).isFalse();
    }
  }

  private SpotGroupAllocator createSpotGroupAllocator(
      Collection<String> virtualInstanceIds) {
    return new SpotGroupAllocator(allocationHelper, ec2Client, stsClient, false,
        createEC2InstanceTemplate(), virtualInstanceIds, 0);
  }

  private EC2InstanceTemplate createEC2InstanceTemplate() {
    Map<String, String> instanceTemplateConfigMap = new LinkedHashMap<>();
    String templateName = "test-template";
    putConfig(instanceTemplateConfigMap, INSTANCE_NAME_PREFIX, templateName);
    putConfig(instanceTemplateConfigMap, IMAGE, "ami-test");
    putConfig(instanceTemplateConfigMap, SECURITY_GROUP_IDS, "sg-test");
    putConfig(instanceTemplateConfigMap, SUBNET_ID, "sb-test");
    putConfig(instanceTemplateConfigMap, TYPE, "m3.medium");
    putConfig(instanceTemplateConfigMap, USE_SPOT_INSTANCES, "true");
    putConfig(instanceTemplateConfigMap, SPOT_BID_USD_PER_HR, "0.1");

    Map<String, String> instanceTemplateTags = new LinkedHashMap<>();
    instanceTemplateTags.put(Tags.InstanceTags.OWNER.getTagKey(), "test-user");

    return new EC2InstanceTemplate(
        templateName, new SimpleConfiguration(instanceTemplateConfigMap), instanceTemplateTags,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }
}
