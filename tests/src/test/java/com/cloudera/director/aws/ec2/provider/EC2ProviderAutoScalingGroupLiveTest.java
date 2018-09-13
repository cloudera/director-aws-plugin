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

package com.cloudera.director.aws.ec2.provider;

import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.AUTOMATIC_INSTANCE_PROCESSING;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.CENTOS67_HVM_AMI_BY_REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.getEc2Provider;
import static com.cloudera.director.aws.test.EC2ProviderCommon.isLive;
import static com.cloudera.director.aws.test.EC2ProviderCommon.putConfig;
import static com.cloudera.director.aws.test.EC2ProviderCommon.waitUntilRunningOrTerminal;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.AUTOMATIC;
import static com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate.SimpleResourceTemplateConfigurationPropertyToken.GROUP_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.common.Callables2;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.allocation.asg.AutoScalingGroupAllocator;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.model.SuspendedProcess;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Filter;
import com.cloudera.director.aws.test.LiveTestProperties;
import com.cloudera.director.aws.test.TestInstanceTemplate;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.exception.AbstractPluginException;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.assertj.core.util.Sets;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link EC2Provider}.
 */
public class EC2ProviderAutoScalingGroupLiveTest {

  private static final Logger LOG = LoggerFactory.getLogger(EC2ProviderAutoScalingGroupLiveTest.class);

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

  /**
   * Will fail the test if there are running instances of the specified type.
   *
   * @param ec2Provider an EC2Provider
   * @param instanceType the instance type to look for
   */
  public void assertNoRunningInstances(EC2Provider ec2Provider, String instanceType) {
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
  public void testAutoScalingGroup() throws InterruptedException {

    // This test will exercise auto scaling group behavior in the EC2Provider.

    String groupId = "testAsg" + UUID.randomUUID().toString();

    boolean enableAutomaticInstanceProcessing = new Random().nextBoolean();
    Set<String> expectedSuspendedProcesses = enableAutomaticInstanceProcessing
        ? Collections.emptySet()
        : Sets.newHashSet(Arrays.asList(
        AutoScalingGroupAllocator.SCALING_PROCESS_REPLACE_UNHEALTHY,
        AutoScalingGroupAllocator.SCALING_PROCESS_AZ_REBALANCE));

    String region = liveTestProperties.getTestRegion();
    String securityGroup = liveTestProperties.getTestSecurityGroup();
    String subnet = liveTestProperties.getTestSubnet();

    LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
    putConfig(providerConfigMap, REGION, region);

    EC2Provider ec2Provider = getEc2Provider(new SimpleConfiguration(providerConfigMap), createCredentialsProvider());

    String image = CENTOS67_HVM_AMI_BY_REGION.get(region).getId();
    TestInstanceTemplate template = new TestInstanceTemplate();
    template.addConfig(IMAGE, image);
    template.addConfig(SECURITY_GROUP_IDS, securityGroup);
    template.addConfig(SUBNET_ID, subnet);

    String instanceType = "m3.medium";
    template.addConfig(TYPE, instanceType);
    template.addConfig(AUTOMATIC, "true");
    template.addConfig(GROUP_ID, groupId);
    if (enableAutomaticInstanceProcessing) {
      template.addConfig(AUTOMATIC_INSTANCE_PROCESSING, "true");
    }

    EC2InstanceTemplate instanceTemplate = ec2Provider.createResourceTemplate(
        template.getTemplateName(), new SimpleConfiguration(template.getConfigs()),
        template.getTags()
    );

    List<String> requestIds1 = Arrays.asList(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    );
    int count = requestIds1.size();

    Callables2.callAll(
        () -> {

          try {
            Collection<EC2Instance> instances = ec2Provider.allocate(instanceTemplate, requestIds1, count);
            assertTrue(count == instances.size());
            // verify idempotency by reusing the same group ID
            instances = ec2Provider.allocate(instanceTemplate, requestIds1, count);
            assertTrue(count == instances.size());

            List<String> ec2InstanceIds = instances.stream()
                .map((i) -> i.unwrap().getInstanceId()).collect(Collectors.toList());

            List<AutoScalingGroup> autoScalingGroups =
                ec2Provider.getAutoScalingClient().describeAutoScalingGroups(
                    new DescribeAutoScalingGroupsRequest()
                        .withAutoScalingGroupNames(groupId))
                    .getAutoScalingGroups();
            assertThat(autoScalingGroups.size()).isEqualTo(1);
            AutoScalingGroup autoScalingGroup = autoScalingGroups.get(0);
            assertThat(autoScalingGroup.getAutoScalingGroupName()).isEqualTo(groupId);

            // Verify that the appropriate auto scaling processes were suspended.
            Set<String> suspendedProcessNames = autoScalingGroup.getSuspendedProcesses().stream()
                .map(SuspendedProcess::getProcessName)
                .collect(Collectors.toSet());
            assertThat(suspendedProcessNames).isEqualTo(expectedSuspendedProcesses);

            Map<String, InstanceState> result =
                waitUntilRunningOrTerminal(ec2Provider, instanceTemplate, ec2InstanceIds);
            for (InstanceState state : result.values()) {
              assertTrue(state.getInstanceStatus().equals(InstanceStatus.RUNNING));
            }
          } catch (AbstractPluginException e) {
            logPluginException(e);
            throw e;
          }
          return null;
        },
        () -> {
          try {
            ec2Provider.delete(instanceTemplate, requestIds1);
          } catch (AbstractPluginException e) {
            logPluginException(e);
            throw e;
          }
          return null;
        }
    );
  }

  private void logPluginException(AbstractPluginException e) {
    LOG.error(e.getLocalizedMessage(), e);
    LOG.error(e.getDetails().getConditionsByKey().toString());
  }
}
