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

package com.cloudera.director.aws.rds;

import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ALLOCATED_STORAGE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.BACKUP_RETENTION_PERIOD;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_SUBNET_GROUP_NAME;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.INSTANCE_CLASS;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.SKIP_FINAL_SNAPSHOT;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.STORAGE_ENCRYPTED;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.VPC_SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.rds.RDSProvider.RDSProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.spi.v2.database.DatabaseServerInstanceTemplate.DatabaseServerInstanceTemplateConfigurationPropertyToken.ADMIN_PASSWORD;
import static com.cloudera.director.spi.v2.database.DatabaseServerInstanceTemplate.DatabaseServerInstanceTemplateConfigurationPropertyToken.ADMIN_USERNAME;
import static com.cloudera.director.spi.v2.database.DatabaseServerInstanceTemplate.DatabaseServerInstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.spi.v2.model.InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.Tags.InstanceTags;
import com.cloudera.director.aws.common.AmazonIdentityManagementClientProvider;
import com.cloudera.director.aws.common.AmazonRDSClientProvider;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfiguration;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfigurationFactory;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.spi.v2.database.DatabaseType;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link RDSProvider}.
 */
public class RDSProviderTest {

  private static final Logger LOG = Logger.getLogger(RDSProviderTest.class.getName());

  // live test properties
  private static String testRegion;
  private static String testDbSubnetGroupName;
  private static String testVpcSecurityGroupIds;
  private static String rdsRegionEndpoint;

  private static boolean isLive() {
    String liveString = System.getProperty("test.aws.live");
    return Boolean.parseBoolean(liveString);
  }

  private static String getProperty(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null) {
      fail("Could not find " + key + " in properties file");
    }
    return value;
  }

  @BeforeClass
  public static void setup() {
    if (isLive()) {
      String liveTestPropertyFile = System.getProperty("test.aws.live.file");

      if (liveTestPropertyFile == null) {
        fail("live test file property is not set");
      }

      Properties prop = new Properties();
      try(InputStream input = new FileInputStream(liveTestPropertyFile)) {
        prop.load(input);

        testRegion = getProperty(prop, "region");
        testDbSubnetGroupName = getProperty(prop, "db_subnet_group_name");
        testVpcSecurityGroupIds = getProperty(prop, "vpc_security_group_ids");
        rdsRegionEndpoint = getProperty(prop, "rds_region_endpoint");
      } catch (IOException e) {
        e.printStackTrace();
        fail("Could not load live test properties file with path " + liveTestPropertyFile);
      }
    }
  }

  @Test
  public void testCreateRDSInstance() throws InterruptedException {
    boolean success = true;

    // TODO change behavior to use profiles to distinguish live tests
    if (isLive()) {
      String username = System.getProperty("user.name");

      // Configure provider
      Map<String, String> providerConfigMap = new LinkedHashMap<>();
      putConfig(providerConfigMap, REGION, testRegion);

      // Configure endpoints
      RDSEndpoints endpoints =
          RDSEndpoints.getTestInstance(ImmutableMap.of(testRegion, rdsRegionEndpoint),
              DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure encryption instance classes
      RDSEncryptionInstanceClasses encryptionInstanceClasses =
          RDSEncryptionInstanceClasses.getTestInstance(ImmutableList.of("db.m3.large"),
                                                       DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure credentials
      Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
      AWSCredentialsProvider credentialsProvider =
          new AWSCredentialsProviderChainProvider().createCredentials(credentialsConfig,
              DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
      ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();

      // Create provider
      RDSProvider rdsProvider = new RDSProvider(
          new SimpleConfiguration(providerConfigMap),
          encryptionInstanceClasses,
          new AmazonRDSClientProvider(credentialsProvider, clientConfiguration, endpoints),
          new AmazonIdentityManagementClientProvider(credentialsProvider, clientConfiguration),
          new CustomTagMappings(null),
          DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure instance template
      Map<String, String> instanceTemplateConfigMap = new LinkedHashMap<>();
      String templateName = username + "-test";
      putConfig(instanceTemplateConfigMap, INSTANCE_NAME_PREFIX, templateName);

      putConfig(instanceTemplateConfigMap, TYPE, DatabaseType.MYSQL.name());
      putConfig(instanceTemplateConfigMap, ADMIN_USERNAME, "admin");
      putConfig(instanceTemplateConfigMap, ADMIN_PASSWORD, "password");

      putConfig(instanceTemplateConfigMap, ALLOCATED_STORAGE, "5");
      putConfig(instanceTemplateConfigMap, INSTANCE_CLASS, "db.m3.medium");
      putConfig(instanceTemplateConfigMap, DB_SUBNET_GROUP_NAME, testDbSubnetGroupName);
      putConfig(instanceTemplateConfigMap, VPC_SECURITY_GROUP_IDS, testVpcSecurityGroupIds);

      putConfig(instanceTemplateConfigMap, BACKUP_RETENTION_PERIOD, "0");
      putConfig(instanceTemplateConfigMap, SKIP_FINAL_SNAPSHOT, "true");
      putConfig(instanceTemplateConfigMap, STORAGE_ENCRYPTED, "true");

      Map<String, String> instanceTemplateTags = new LinkedHashMap<>();
      instanceTemplateTags.put(InstanceTags.OWNER.getTagKey(), username);

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
            EnumSet.of(InstanceStatus.DELETED, InstanceStatus.FAILED, InstanceStatus.STOPPED)
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
          EnumSet.of(InstanceStatus.DELETED, InstanceStatus.UNKNOWN),
          EnumSet.of(InstanceStatus.FAILED, InstanceStatus.STOPPED)
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
