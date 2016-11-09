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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_KMS_KEY_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_COUNT;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_SIZE_GIB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ENCRYPT_ADDITIONAL_EBS_VOLUMES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_TYPE;
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
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProviderChain;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Tag;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.VolumeState;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Volume;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.kms.AWSKMSClient;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests {@link EC2Provider}.
 */
@RunWith(Parameterized.class)
public class EC2ProviderTest {

  private static final Logger LOG = Logger.getLogger(EC2ProviderTest.class.getName());

  private static final String VOLUME_NOT_FOUND_ERR_CODE = "InvalidVolume.NotFound";

  private static final String KMS_KEY_ARN =
      "arn:aws:kms:us-west-1:666144601417:key/555d7e4f-9b56-4393-814d-63d5c5117cfe";

 @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { false, false, false },  // on-demand instances
      { false, true, false },   // on-demand instances + ebs volumes
      { false, true, true },    // on-demand instances + ebs volumes + ebs encryption
      { true, false, false },   // spot instances
      { true, true, false }     // spot instances + ebs volumes
    });
  }

  private boolean useSpotInstances;
  private boolean useEbsVolumes;
  private boolean testEbsEncryption;

  public EC2ProviderTest(boolean useSpotInstances, boolean useEbsVolumes, boolean testEbsEncryption) {
    this.useSpotInstances = useSpotInstances;
    this.useEbsVolumes = useEbsVolumes;
    this.testEbsEncryption = testEbsEncryption;
  }

  @Test
  public void exerciseCreateEC2Instance() throws InterruptedException {

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

      // Configure ebs metadata
      EBSMetadata ebsMetadata =
          EBSMetadata.getDefaultInstance(ImmutableMap.of("st1", "500-16384"),
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
          new AWSCredentialsProviderChainProvider(null).createCredentials(credentialsConfig,
              DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Create provider
      EC2Provider ec2Provider = new EC2Provider(new SimpleConfiguration(providerConfigMap),
          ephemeralDeviceMappings, ebsMetadata, virtualizationMappings, awsFilters, new AmazonEC2Client(providerChain),
          new AmazonIdentityManagementClient(providerChain), new AWSKMSClient(providerChain),
          DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

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

      int ebsVolumeCount = 0;
      int ebsVolumeSizeGiB = 500;
      String ebsVolumeType = "st1";
      String enableEbsEncryption = "true";

      Set<String> mountedEbsVolumes = Sets.newHashSet();
      if (useEbsVolumes) {
        ebsVolumeCount = 2;
        putConfig(instanceTemplateConfigMap, EBS_VOLUME_COUNT, String.valueOf(ebsVolumeCount));
        putConfig(instanceTemplateConfigMap, EBS_VOLUME_SIZE_GIB, String.valueOf(ebsVolumeSizeGiB));
        putConfig(instanceTemplateConfigMap, EBS_VOLUME_TYPE, ebsVolumeType);

        if (testEbsEncryption) {
          putConfig(instanceTemplateConfigMap, ENCRYPT_ADDITIONAL_EBS_VOLUMES, enableEbsEncryption);
          putConfig(instanceTemplateConfigMap, EBS_KMS_KEY_ID, KMS_KEY_ARN);
        }
      }

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

        if (useEbsVolumes) {
          for (String virtualInstanceId : instanceStates.keySet()) {
            LOG.info("Checking that volumes were properly mounted for instance " + virtualInstanceId);
            List<Volume> volumes = ec2Provider.getVolumes(virtualInstanceId);

            int expectedVolumeCount = ebsVolumeCount + 1; // plus 1 for root volume
            if (volumes.size() != expectedVolumeCount) {
              LOG.severe("Expected " + expectedVolumeCount + " volumes but got " + volumes.size());
              success = false;
              break;
            }

            int rootVolumeCount = 0;
            int mountedVolumeCount = 0;
            for (Volume volume : volumes) {
              String volumeType = volume.getVolumeType();

              if (volumeType.equals(ebsVolumeType)) {
                mountedVolumeCount++;
                mountedEbsVolumes.add(volume.getVolumeId());
                assertTrue(volume.getSize() == ebsVolumeSizeGiB);

                if (testEbsEncryption) {
                  assertTrue(volume.getEncrypted());
                  assertTrue(volume.getKmsKeyId().equals(KMS_KEY_ARN));
                }

                // EBS volumes should also have the tags from instance template
                Map<String, String> volumeTags = listTagToMap(volume.getTags());
                for (Map.Entry<String, String> entry : instanceTemplateTags.entrySet()) {
                  assertTrue(volumeTags.containsKey(entry.getKey()));
                  assertTrue(volumeTags.containsValue(entry.getValue()));
                }
              } else if (volumeType.equals(ROOT_VOLUME_TYPE.unwrap().getDefaultValue())) {
                rootVolumeCount++;
              } else {
                success = false;
                LOG.severe("Found unexpected volume type " + volumeType);
              }
            }

            if (rootVolumeCount != 1) {
              LOG.severe("Found more than one volume with root volume type");
              success = false;
            }

            if (mountedVolumeCount != ebsVolumeCount) {
              LOG.severe("Incorrect number of ebs volumes");
              success = false;
            }
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

      if (useEbsVolumes) {
        Set<String> pendingEbsVolumes = Sets.newHashSet(mountedEbsVolumes);

        LOG.info("Verifying that the provisioned EBS volumes are deleted");
        while (true) {
          pendingEbsVolumes = getUndeletedVolumes(ec2Provider.getClient(), pendingEbsVolumes);

          if (pendingEbsVolumes.isEmpty()) break;

          LOG.info(pendingEbsVolumes.size()
              + " EBS volumes still pending to be deleted. Sleeping for 15 seconds...");
          Thread.sleep(15000);
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

  private Map<String, String> listTagToMap(List<Tag> tags) {
    Map<String, String> tagMap = Maps.newHashMap();
    for (Tag tag : tags) {
      tagMap.put(tag.getKey(), tag.getValue());
    }
    return tagMap;
  }

  // Return the number of EBS volumes that are not yet deleted.
  // Volumes that can't be found are considered deleted.
  public Set<String> getUndeletedVolumes(AmazonEC2Client client, Collection<String> volumeIds) {
    Set<String> undeleted = Sets.newHashSet();

    for (String id : volumeIds) {
      DescribeVolumesRequest request = new DescribeVolumesRequest().withVolumeIds(id);

      try {
        DescribeVolumesResult volumeResults = client.describeVolumes(request);

        Volume volume = Iterables.getOnlyElement(volumeResults.getVolumes());
        String volumeState = volume.getState();

        if (!volumeState.equals(VolumeState.Deleted.toString())) {
          LOG.info("Volume " + volumeIds + " still has state " + volumeState);
          undeleted.add(volume.getVolumeId());
        }
      } catch (AmazonServiceException ex) {
        if (!ex.getErrorCode().equals(VOLUME_NOT_FOUND_ERR_CODE)) {
          throw ex;
        }
      }
    }
    return undeleted;
  }
}
