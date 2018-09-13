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

package com.cloudera.director.aws.test;

import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.aws.clientprovider.AWSKMSClientProvider;
import com.cloudera.director.aws.clientprovider.AmazonAutoScalingClientProvider;
import com.cloudera.director.aws.clientprovider.AmazonEC2ClientProvider;
import com.cloudera.director.aws.clientprovider.AmazonIdentityManagementClientProvider;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.provider.EC2Provider;
import com.cloudera.director.aws.ec2.provider.EC2ProviderParameterizedLiveTest;
import com.cloudera.director.aws.ec2.EphemeralDeviceMappings;
import com.cloudera.director.aws.ec2.VirtualizationMappings;
import com.cloudera.director.aws.ec2.ebs.EBSDeviceMappings;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfiguration;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfigurationFactory;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigValueFactory;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static com.cloudera.director.spi.v2.provider.Launcher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;

/**
 * Contains common functions used to test EC2Provider.
 */
public class EC2ProviderCommon {

  private static final Logger LOG = Logger.getLogger(EC2ProviderParameterizedLiveTest.class.getName());

  private static final String CUSTOM_TEST_NAME_TAG = "TestName";
  public static final String EXTRA_TAG = "extra";
  private static final String CUSTOM_TEST_EXTRA_TAG = "TestExtra";

  private static final Config CUSTOM_TAG_MAPPING_CONFIG =
      ConfigValueFactory.fromMap(ImmutableMap.<String, String>builder()
          .put(Tags.ResourceTags.RESOURCE_NAME.getTagKey(), CUSTOM_TEST_NAME_TAG)
          .put(EXTRA_TAG, CUSTOM_TEST_EXTRA_TAG)
          .build()).toConfig();

  /**
   * Returns whether the live test property is true
   *
   * @return whether the live test property is true
   */
  public static boolean isLive() {
    String liveString = System.getProperty("test.aws.live");
    return Boolean.parseBoolean(liveString);
  }

  /**
   * Waits until the instance state is running or has a terminal state. Returns
   * the resulting instance states.
   *
   * @param ec2Provider              the EC2 provider
   * @param instanceTemplate         the instance template
   * @param virtualInstanceIds       the virtual instance ids
   * @return the map from virtual instance ID to instance state
   * @throws InterruptedException if the process is interrupted
   */
  public static Map<String, InstanceState> waitUntilRunningOrTerminal(EC2Provider ec2Provider,
      EC2InstanceTemplate instanceTemplate, Collection<String> virtualInstanceIds) throws InterruptedException {
    return waitForInstanceStates(
        ec2Provider,
        instanceTemplate,
        virtualInstanceIds,
        EnumSet.of(InstanceStatus.RUNNING),
        EnumSet.of(InstanceStatus.DELETED, InstanceStatus.FAILED, InstanceStatus.STOPPED)
    );
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
  public static Map<String, InstanceState> waitForInstanceStates(EC2Provider ec2Provider,
                                                                 EC2InstanceTemplate instanceTemplate,
                                                                 Collection<String> virtualInstanceIds,
     Collection<InstanceStatus> desiredInstanceStatuses, Collection<InstanceStatus> terminalInstanceStatuses)
      throws InterruptedException {
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

  /**
   * Puts a configuration property and value into a configuration map
   *
   * @param configMap the configuration map
   * @param propertyToken the configuration property token
   * @param value the configuration value
   */
  public static void putConfig(Map<String, String> configMap, ConfigurationPropertyToken propertyToken,
                                String value) {
    if (value != null) {
      configMap.put(propertyToken.unwrap().getConfigKey(), value);
    }
  }

  /**
   * Returns an instance of the @{@link EC2Provider} with the specified configs.
   *
   * @param configuration the configurations
   * @param credentialsProvider the AWS credentials provider
   * @return an instance of the @{@link EC2Provider}
   */
  public static EC2Provider getEc2Provider(Configured configuration,
                                           AWSCredentialsProvider credentialsProvider) {
    return getEc2Provider(configuration, credentialsProvider,
        new AmazonEC2ClientProvider(credentialsProvider, new ClientConfigurationFactory().getConfig()));
  }

  /**
   * Returns an instance of the @{@link EC2Provider} with the specified configs.
   *
   * @param configuration the configurations
   * @param credentialsProvider the AWS credentials provider
   * @param amazonEC2ClientProvider the AWS EC2 client provider
   * @return an instance of the @{@link EC2Provider}
   */
  public static EC2Provider getEc2Provider(Configured configuration,
      AWSCredentialsProvider credentialsProvider, AmazonEC2ClientProvider amazonEC2ClientProvider) {
    // Configure ephemeral device mappings
    EphemeralDeviceMappings ephemeralDeviceMappings =
        EphemeralDeviceMappings.getTestInstance(ImmutableMap.of("m3.medium", 1, "i3.4xlarge", 2),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure ebs device mappings
    EBSDeviceMappings ebsDeviceMappings =
        EBSDeviceMappings.getDefaultInstance(ImmutableMap.<String, String>of(),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure ebs metadata
    EBSMetadata ebsMetadata =
        EBSMetadata.getDefaultInstance(ImmutableMap.of("st1", "500-16384"),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure virtualization mappings
    VirtualizationMappings virtualizationMappings =
        VirtualizationMappings.getTestInstance(ImmutableMap.of("paravirtual", Arrays.asList("m3.medium")),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure filters and timeouts
    AWSFilters awsFilters = AWSFilters.EMPTY_FILTERS;
    AWSTimeouts awsTimeouts = new AWSTimeouts(null);
    CustomTagMappings customTagMappings = new CustomTagMappings(CUSTOM_TAG_MAPPING_CONFIG);
    ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();

    return new EC2Provider(
        configuration,
        ephemeralDeviceMappings,
        ebsDeviceMappings,
        ebsMetadata,
        virtualizationMappings,
        awsFilters,
        awsTimeouts,
        customTagMappings,
        NetworkRules.EMPTY_RULES,
        amazonEC2ClientProvider,
        new AmazonAutoScalingClientProvider(credentialsProvider, clientConfiguration),
        new AmazonIdentityManagementClientProvider(credentialsProvider, clientConfiguration),
        new AWSKMSClientProvider(credentialsProvider, clientConfiguration),
        true,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  // TODO : extract AMI out to properties file

  public static final Map<String, Ami> CENTOS64_AMI_BY_REGION = ImmutableMap.of(
      "us-west-1", new Ami("ami-5d456c18", "us-west-1", false),
      "us-west-2", new Ami("ami-b3bf2f83", "us-west-2", false)
  );

  public static final Map<String, Ami> CENTOS67_HVM_AMI_BY_REGION = ImmutableMap.of(
      "us-west-1", new Ami("ami-ac5f2fcc", "us-west-1", false),
      "us-west-2", new Ami("ami-05cf2265", "us-west-2", false)
  );

  public static final Map<String, Ami> RHEL67_AMI_BY_REGION = ImmutableMap.of(
      "us-west-1", new Ami("ami-5b8a781f", "us-west-1", true),
      "us-west-2", new Ami("ami-75f3f145", "us-west-2", true)
  );

  /**
   * Describes an AMI.
   */
  public static class Ami {
    private String id;
    private String region;
    private boolean supportsHostKeyFingerprintRetrieval;

    public Ami(String id, String region, boolean supportsHostKeyFingerprintRetrieval) {
      this.id = id;
      this.region = region;
      this.supportsHostKeyFingerprintRetrieval = supportsHostKeyFingerprintRetrieval;
    }

    public String getId() {
      return id;
    }

    public String getRegion() {
      return region;
    }

    public boolean isSupportsHostKeyFingerprintRetrieval() {
      return supportsHostKeyFingerprintRetrieval;
    }
  }

}
