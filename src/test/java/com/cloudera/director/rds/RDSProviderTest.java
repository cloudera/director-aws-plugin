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

package com.cloudera.director.rds;

import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ALLOCATED_STORAGE;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_SUBNET_GROUP_NAME;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.INSTANCE_CLASS;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.VPC_SECURITY_GROUP_IDS;
import static com.cloudera.director.rds.RDSProvider.RDSProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.spi.v1.database.DatabaseServerInstanceTemplate.DatabaseServerInstanceTemplateConfigurationPropertyToken.ADMIN_PASSWORD;
import static com.cloudera.director.spi.v1.database.DatabaseServerInstanceTemplate.DatabaseServerInstanceTemplateConfigurationPropertyToken.ADMIN_USERNAME;
import static com.cloudera.director.spi.v1.database.DatabaseServerInstanceTemplate.DatabaseServerInstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.spi.v1.model.InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.cloudera.director.InstanceTags;
import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.spi.v1.database.DatabaseType;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link RDSProvider}.
 */
public class RDSProviderTest {

  private static final Logger LOG = LoggerFactory.getLogger(RDSProviderTest.class);

  @Test
  public void testCreateRDSInstance() throws InterruptedException {

    String liveString = System.getProperty("test.aws.live");
    boolean live = Boolean.parseBoolean(liveString);

    boolean success = true;

    // TODO change behavior to use profiles to distinguish live tests
    if (live) {
      String username = System.getProperty("user.name");

      // Configure provider
      Map<String, String> providerConfigMap = new LinkedHashMap<String, String>();
      putConfig(providerConfigMap, REGION, "us-west-1");

      // Configure credentials
      Map<String, String> credentialsConfigMap = new LinkedHashMap<String, String>();
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
      AWSCredentialsProviderChain providerChain =
          new AWSCredentialsProviderChainProvider().createCredentials(credentialsConfig,
              DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Create provider
      RDSProvider rdsProvider = new RDSProvider(new SimpleConfiguration(providerConfigMap),
          new AmazonRDSClient(providerChain), new AmazonIdentityManagementClient(providerChain),
          DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure instance template
      Map<String, String> instanceTemplateConfigMap = new LinkedHashMap<String, String>();
      String templateName = username + "-test";
      putConfig(instanceTemplateConfigMap, INSTANCE_NAME_PREFIX, templateName);

      putConfig(instanceTemplateConfigMap, TYPE, DatabaseType.MYSQL.name());
      putConfig(instanceTemplateConfigMap, ADMIN_USERNAME, "admin");
      putConfig(instanceTemplateConfigMap, ADMIN_PASSWORD, "password");

      putConfig(instanceTemplateConfigMap, ALLOCATED_STORAGE, "5");
      putConfig(instanceTemplateConfigMap, INSTANCE_CLASS, "db.t2.micro");
      putConfig(instanceTemplateConfigMap, DB_SUBNET_GROUP_NAME, "all-subnets");
      putConfig(instanceTemplateConfigMap, VPC_SECURITY_GROUP_IDS, "sg-4af9292f");

      Map<String, String> instanceTemplateTags = new LinkedHashMap<String, String>();
      instanceTemplateTags.put(InstanceTags.OWNER, username);

      // Create instance template
      RDSInstanceTemplate instanceTemplate = rdsProvider.createResourceTemplate(
          templateName, new SimpleConfiguration(instanceTemplateConfigMap), instanceTemplateTags);

      // Create instances
      List<String> virtualInstanceIds = Arrays.asList(
          String.format("%s-db%s", templateName, UUID.randomUUID().toString()),
          String.format("%s-db%s", templateName, UUID.randomUUID().toString())
      );
      int instanceCount = virtualInstanceIds.size();

      LOG.info("Allocating " + instanceCount + " instances.");
      rdsProvider.allocate(instanceTemplate, virtualInstanceIds, instanceCount);
      Collection<RDSInstance> instances = rdsProvider.find(instanceTemplate, virtualInstanceIds);

      try {
        for (RDSInstance instance : instances) {
          LOG.info("Created instance: " + instance.getProperties());
        }

        // Wait for instances to be running
        LOG.info("Waiting for instances to be running.");
        Map<String, InstanceState> instanceStates = waitForInstanceStates(
            rdsProvider,
            instanceTemplate,
            virtualInstanceIds,
            EnumSet.of(InstanceStatus.RUNNING),
            EnumSet.of(InstanceStatus.DELETED, InstanceStatus.FAILED, InstanceStatus.STOPPED,
                InstanceStatus.UNKNOWN)
        );

        for (Map.Entry<String, InstanceState> entry : instanceStates.entrySet()) {
          String virtualInstanceId = entry.getKey();
          InstanceState instanceState = entry.getValue();
          InstanceStatus instanceStatus = instanceState.getInstanceStatus();
          LOG.info("Instance: " + virtualInstanceId + " has status: " + instanceStatus);
          if (instanceStatus != InstanceStatus.RUNNING) {
            success = false;
          }
        }

      } finally {
        // Terminate instances
        rdsProvider.delete(instanceTemplate, virtualInstanceIds);
      }

      // Wait for instances to be deleted
      LOG.info("Waiting for instances to be deleted.");
      Map<String, InstanceState> instanceStates = waitForInstanceStates(
          rdsProvider,
          instanceTemplate,
          virtualInstanceIds,
          EnumSet.of(InstanceStatus.DELETED),
          EnumSet.of(InstanceStatus.FAILED, InstanceStatus.STOPPED, InstanceStatus.UNKNOWN)
      );

      for (Map.Entry<String, InstanceState> entry : instanceStates.entrySet()) {
        String virtualInstanceId = entry.getKey();
        InstanceState instanceState = entry.getValue();
        InstanceStatus instanceStatus = instanceState.getInstanceStatus();

        LOG.info("Instance: " + virtualInstanceId + " has status: " + instanceStatus);
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
    configMap.put(propertyToken.unwrap().getConfigKey(), value);
  }

  /**
   * Determines instance state for the specified instances, waiting until
   * all instances have a desired or terminal status.
   *
   * @param rdsProvider              the RDS provider
   * @param instanceTemplate         the database server instance template
   * @param virtualInstanceIds       the virtual instance ids
   * @param desiredInstanceStatuses  the desired instance statuses
   * @param terminalInstanceStatuses the terminal instance statuses
   * @return the map from virtual instance ID to instance state
   * @throws InterruptedException if the process is interrupted
   */
  private Map<String, InstanceState> waitForInstanceStates(RDSProvider rdsProvider,
      RDSInstanceTemplate instanceTemplate, Collection<String> virtualInstanceIds,
      Collection<InstanceStatus> desiredInstanceStatuses,
      Collection<InstanceStatus> terminalInstanceStatuses)
      throws InterruptedException {

    Map<String, InstanceState> instanceStates = Maps.newHashMap();
    Set<String> pendingInstanceIds = Sets.newHashSet(virtualInstanceIds);
    while (!pendingInstanceIds.isEmpty()) {
      Map<String, InstanceState> currentInstanceStates =
          rdsProvider.getInstanceState(instanceTemplate, pendingInstanceIds);

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
            + " instance(s) still pending. Sleeping for 30 seconds...");
        Thread.sleep(30000);
      }
    }
    return instanceStates;
  }
}
