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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SPOT_BID_USD_PER_HR;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.spi.v1.model.InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX;
import static org.junit.Assert.assertTrue;

import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.InstanceTags;
import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProviderChain;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.junit.Test;

/**
 * Tests {@link EC2Provider}.
 */
public class EC2ProviderTest {

  private static final Logger LOG = Logger.getLogger(EC2ProviderTest.class.getName());

  @Test
  public void testCreateEC2Instance() throws InterruptedException {
    exerciseCreateEC2Instance(false);
  }

  @Test
  public void testCreateEC2SpotInstance() throws InterruptedException {
    exerciseCreateEC2Instance(true);
  }

  private void exerciseCreateEC2Instance(boolean useSpotInstances) throws InterruptedException {

    String liveString = System.getProperty("test.aws.live");
    boolean live = Boolean.parseBoolean(liveString);

    boolean success = true;

    // TODO change behavior to use profiles to distinguish live tests
    if (live) {

      String ami;
      String spotBidUSDPerHour = null;
      String spotString = "";

      if (useSpotInstances) {
        ami = "ami-5d456c18"; //CentOS 6.4 PV
        spotBidUSDPerHour = "0.50";
        spotString = "Spot ";
      } else {
        ami = "ami-6283a827"; //RHEL 6.4 PV
      }

      String username = System.getProperty("user.name");

      // Configure provider
      LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<String, String>();
      putConfig(providerConfigMap, REGION, "us-west-1");

      // Configure ephemeral device mappings
      EphemeralDeviceMappings ephemeralDeviceMappings =
          EphemeralDeviceMappings.getTestInstance(ImmutableMap.of("m3.medium", 1),
              DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure virtualization mappings
      VirtualizationMappings virtualizationMappings =
          VirtualizationMappings.getTestInstance(ImmutableMap.of("paravirtual", Arrays.asList("m3.medium")),
              DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure filters
      AWSFilters awsFilters = AWSFilters.EMPTY_FILTERS;

      // Configure credentials
      Map<String, String> credentialsConfigMap = new LinkedHashMap<String, String>();
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
      AWSCredentialsProviderChain providerChain =
          new AWSCredentialsProviderChainProvider().createCredentials(credentialsConfig,
              DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Create provider
      EC2Provider ec2Provider = new EC2Provider(new SimpleConfiguration(providerConfigMap),
          ephemeralDeviceMappings, virtualizationMappings, awsFilters, new AmazonEC2Client(providerChain),
          new AmazonIdentityManagementClient(providerChain), DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure instance template
      Map<String, String> instanceTemplateConfigMap = new LinkedHashMap<String, String>();
      String templateName = username + "-test";
      putConfig(instanceTemplateConfigMap, INSTANCE_NAME_PREFIX, templateName);
      putConfig(instanceTemplateConfigMap, IMAGE, ami);
      putConfig(instanceTemplateConfigMap, SECURITY_GROUP_IDS, "sg-4af9292f");
      putConfig(instanceTemplateConfigMap, SUBNET_ID, "subnet-52e6f214");
      putConfig(instanceTemplateConfigMap, TYPE, "m3.medium");
      putConfig(instanceTemplateConfigMap, USE_SPOT_INSTANCES, String.valueOf(useSpotInstances));
      putConfig(instanceTemplateConfigMap, SPOT_BID_USD_PER_HR, spotBidUSDPerHour);
      Map<String, String> instanceTemplateTags = new LinkedHashMap<String, String>();
      instanceTemplateTags.put(InstanceTags.OWNER, username);

      // Create instance template
      EC2InstanceTemplate instanceTemplate = ec2Provider.createResourceTemplate(
          templateName, new SimpleConfiguration(instanceTemplateConfigMap), instanceTemplateTags);

      // Create instances
      List<String> virtualInstanceIds = Arrays.asList(UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      int instanceCount = virtualInstanceIds.size();

      LOG.info("Allocating " + instanceCount + " " + spotString + "instances.");
      ec2Provider.allocate(instanceTemplate, virtualInstanceIds, instanceCount);
      Collection<EC2Instance> instances = ec2Provider.find(instanceTemplate, virtualInstanceIds);

      try {
        for (EC2Instance instance : instances) {
          LOG.info("Created " + spotString + "instance: " + instance.getProperties());
        }

        // Wait for instances to be running
        LOG.info("Waiting for " + spotString + "instances to be running.");
        Map<String, InstanceState> instanceStates = waitForInstanceStates(
            ec2Provider,
            instanceTemplate,
            virtualInstanceIds,
            EnumSet.of(InstanceStatus.RUNNING),
            EnumSet.of(InstanceStatus.DELETED, InstanceStatus.FAILED, InstanceStatus.STOPPED)
        );

        for (Map.Entry<String, InstanceState> entry : instanceStates.entrySet()) {
          String virtualInstanceId = entry.getKey();
          InstanceState instanceState = entry.getValue();
          InstanceStatus instanceStatus = instanceState.getInstanceStatus();

          LOG.info(spotString + "Instance: " + virtualInstanceId + " has status: " + instanceStatus);
          if (instanceStatus != InstanceStatus.RUNNING) {
            success = false;
          }
        }

      } finally {
        // Terminate instances
        ec2Provider.delete(instanceTemplate, virtualInstanceIds);
      }

      // Wait for instances to be deleted
      LOG.info("Waiting for " + spotString + "instances to be deleted.");
      Map<String, InstanceState> instanceStates = waitForInstanceStates(
          ec2Provider,
          instanceTemplate,
          virtualInstanceIds,
          EnumSet.of(InstanceStatus.DELETED, InstanceStatus.UNKNOWN),
          EnumSet.of(InstanceStatus.FAILED, InstanceStatus.STOPPED)
      );

      for (Map.Entry<String, InstanceState> entry : instanceStates.entrySet()) {
        String virtualInstanceId = entry.getKey();
        InstanceState instanceState = entry.getValue();
        InstanceStatus instanceStatus = instanceState.getInstanceStatus();
        LOG.info(spotString + "Instance: " + virtualInstanceId + " has status: " + instanceStatus);
        if ((instanceStatus != InstanceStatus.DELETED)
            && (instanceStatus != InstanceStatus.UNKNOWN)) {
          success = false;
        }
      }
    }

    assertTrue(success);
  }

  private void putConfig(Map<String, String> configMap, ConfigurationPropertyToken propertyToken,
      String value) {
    if (value != null) {
      configMap.put(propertyToken.unwrap().getConfigKey(), value);
    }
  }

  /**
   * Determines instance state for the specified instances, waiting until
   * all instances have a desired or terminal status.
   *
   * @param ec2Provider              the EC2 provider
   * @param instanceTemplate         the instance template
   * @param virtualInstanceIds       the virtual instance ids
   * @param desiredInstanceStatuses  the desired instance statuses
   * @param terminalInstanceStatuses the terminal instance statuses
   * @return the map from virtual instance ID to instance state
   * @throws InterruptedException if the process is interrupted
   */
  private Map<String, InstanceState> waitForInstanceStates(EC2Provider ec2Provider,
      EC2InstanceTemplate instanceTemplate, Collection<String> virtualInstanceIds,
      Collection<InstanceStatus> desiredInstanceStatuses,
      Collection<InstanceStatus> terminalInstanceStatuses) throws InterruptedException {

    Map<String, InstanceState> instanceStates = Maps.newHashMap();
    Set<String> pendingInstanceIds = Sets.newHashSet(virtualInstanceIds);
    while (!pendingInstanceIds.isEmpty()) {
      Map<String, InstanceState> currentInstanceStates =
          ec2Provider.getInstanceState(instanceTemplate, pendingInstanceIds);

      for (Map.Entry<String, InstanceState> entry : currentInstanceStates.entrySet()) {
        String virtualInstanceId = entry.getKey();
        InstanceState instanceState = entry.getValue();
        InstanceStatus instanceStatus = instanceState.getInstanceStatus();

        if (desiredInstanceStatuses.contains(instanceStatus)) {
          LOG.info("Instance: " + virtualInstanceId + " has desired status: " + instanceStatus);
          instanceStates.put(virtualInstanceId, instanceState);
          pendingInstanceIds.remove(virtualInstanceId);

        } else if (terminalInstanceStatuses.contains(instanceStatus)) {
          LOG.info("Instance: " + virtualInstanceId + " has terminal status: " + instanceStatus);
          instanceStates.put(virtualInstanceId, instanceState);
          pendingInstanceIds.remove(virtualInstanceId);
        }

      }
      if (!pendingInstanceIds.isEmpty()) {
        LOG.info(pendingInstanceIds.size()
            + " instance(s) still pending. Sleeping for 15 seconds...");
        Thread.sleep(15000);
      }
    }
    return instanceStates;
  }
}
