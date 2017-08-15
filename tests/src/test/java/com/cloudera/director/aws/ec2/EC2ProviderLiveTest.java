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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.BLOCK_DURATION_MINUTES;
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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USER_DATA;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.spi.v1.model.InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.aws.Tags.InstanceTags;
import com.cloudera.director.aws.common.AWSKMSClientProvider;
import com.cloudera.director.aws.common.AmazonEC2ClientProvider;
import com.cloudera.director.aws.common.AmazonIdentityManagementClientProvider;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfiguration;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfigurationFactory;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceAttribute;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceAttributeName;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Tag;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Volume;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.VolumeState;
import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigValueFactory;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyValue;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests {@link EC2Provider}.
 */
@RunWith(Enclosed.class)
public class EC2ProviderLiveTest {

  private static final Logger LOG = Logger.getLogger(EC2ProviderLiveTest.class.getName());

  private static final String CUSTOM_TEST_NAME_TAG = "TestName";
  private static final String EXTRA_TAG = "extra";
  private static final String CUSTOM_TEST_EXTRA_TAG = "TestExtra";

  private static final Config CUSTOM_TAG_MAPPING_CONFIG =
      ConfigValueFactory.fromMap(ImmutableMap.<String, String>builder()
          .put(Tags.ResourceTags.RESOURCE_NAME.getTagKey(), CUSTOM_TEST_NAME_TAG)
          .put(EXTRA_TAG, CUSTOM_TEST_EXTRA_TAG)
          .build()).toConfig();

  private static boolean isLive() {
    String liveString = System.getProperty("test.aws.live");
    return Boolean.parseBoolean(liveString);
  }

  private static void putConfig(Map<String, String> configMap, ConfigurationPropertyToken propertyToken,
                                String value) {
    if (value != null) {
      configMap.put(propertyToken.unwrap().getConfigKey(), value);
    }
  }

  private static EC2Provider getEc2Provider(Configured configuration,
      AWSCredentialsProvider credentialsProvider) {
    return getEc2Provider(configuration, credentialsProvider,
        new AmazonEC2ClientProvider(credentialsProvider, new ClientConfigurationFactory().getConfig()));
  }

  private static EC2Provider getEc2Provider(Configured configuration,
      AWSCredentialsProvider credentialsProvider, AmazonEC2ClientProvider amazonEC2ClientProvider) {
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

    // Configure filters and timeouts
    AWSFilters awsFilters = AWSFilters.EMPTY_FILTERS;
    AWSTimeouts awsTimeouts = new AWSTimeouts(null);
    CustomTagMappings customTagMappings = new CustomTagMappings(CUSTOM_TAG_MAPPING_CONFIG);
    ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();

    return new EC2Provider(
        configuration,
        ephemeralDeviceMappings,
        ebsMetadata,
        virtualizationMappings,
        awsFilters,
        awsTimeouts,
        customTagMappings,
        NetworkRules.EMPTY_RULES,
        amazonEC2ClientProvider,
        new AmazonIdentityManagementClientProvider(credentialsProvider, clientConfiguration),
        new AWSKMSClientProvider(credentialsProvider, clientConfiguration),
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  public static class NotParameterizedTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCreateEC2InstanceWithExcessiveTags() throws InterruptedException {
      assumeTrue(isLive());

      Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
      AWSCredentialsProvider credentialsProvider =
          new AWSCredentialsProviderChainProvider()
              .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

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
      ec2Provider.allocate(template, ImmutableList.of("virtualId"), 1);
    }
  }

  @RunWith(Parameterized.class)
  public static class CreateProviderTest {

    // This test makes sure we can successfully instantiate the EC2Provider for
    // all valid regions. This ensures that the region endpoints can be set for
    // all the AWS clients used in the EC2Provider.

    private EC2Provider createEC2Provider(String region) {
      Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
      AWSCredentialsProvider credentialsProvider =
          new AWSCredentialsProviderChainProvider()
              .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      // Configure provider
      LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
      putConfig(providerConfigMap, REGION, region);

      // Create provider
      return getEc2Provider(new SimpleConfiguration(providerConfigMap), credentialsProvider);
    }

    @Parameterized.Parameters
    public static Iterable<? extends Object> data() {
      List<String> regionsToTest = new ArrayList<>();

      for (ConfigurationPropertyValue config : REGION.unwrap().getValidValues(DEFAULT_PLUGIN_LOCALIZATION_CONTEXT)) {
        String region = config.getValue();
        regionsToTest.add(region);
      }

      return regionsToTest;
    }

    private String region;

    public CreateProviderTest(String region) {
      this.region = region;
    }

    @Test
    public void testCreateProvider() {
      assumeTrue(isLive());
      createEC2Provider(region);
    }
  }

  @RunWith(Parameterized.class)
  public static class ParameterizedTest {
    private static final String VOLUME_NOT_FOUND_ERR_CODE = "InvalidVolume.NotFound";

    private static final Integer DEFAULT_BLOCK_DURATION_MINUTES = 60;

    private static final String TEST_USER_DATA = "Test user data";

    // live test properties
    private static String testRegion;
    private static String testSubnet;
    private static String testSecurityGroup;
    private static String testKmsKeyArn;

    // live test properties for delegated role access
    private static String delegatedRoleArn;
    private static String delegatedRoleExternalId;
    private static String externalAccountRegion;
    private static String externalAccountSubnetId;
    private static String externalAccountSecurityGroup;
    private static String externalAccountKmsKeyArn;

    private static String liveTestPropertyFile;

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
        liveTestPropertyFile = System.getProperty("test.aws.live.file");

        if (liveTestPropertyFile == null) {
          fail("live test file property is not set");
        }

        Properties prop = new Properties();
        try(InputStream input = new FileInputStream(liveTestPropertyFile)) {
          prop.load(input);

          testRegion = getProperty(prop, "region");
          testSubnet = getProperty(prop, "subnet_id");
          testSecurityGroup = getProperty(prop, "security_group");
          testKmsKeyArn = getProperty(prop, "kms_key_arn");

          delegatedRoleArn = getProperty(prop, "delegated_role_arn");
          delegatedRoleExternalId = getProperty(prop, "delegated_role_external_id");
          externalAccountRegion = getProperty(prop, "external_account_region");
          externalAccountSubnetId = getProperty(prop, "external_account_subnet_id");
          externalAccountSecurityGroup = getProperty(prop, "external_account_security_group");
          externalAccountKmsKeyArn = getProperty(prop, "external_account_kms_key");
        } catch (IOException e) {
          e.printStackTrace();
          fail("Could not load live test properties file with path " + liveTestPropertyFile);
        }
      }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{
          {false, false, false, false, false},  // on-demand instances
          {false, false, true, false, false},   // on-demand instances + ebs volumes
          {false, false, true, false, true},    // on-demand instances + ebs volumes + immediate failure
          {false, false, true, true, false},    // on-demand instances + ebs volumes + ebs encryption
          {true, false, false, false, false},   // spot instances
          {true, true, false, false, false},    // spot instances with block duration
          {true, false, true, false, false}     // spot instances + ebs volumes
      });
    }

    private boolean useSpotInstances;
    private boolean useSpotBlock;
    private boolean useEbsVolumes;
    private boolean testEbsEncryption;
    private boolean terminateImmediately;

    public ParameterizedTest(boolean useSpotInstances, boolean useSpotBlock,
        boolean useEbsVolumes, boolean testEbsEncryption, boolean terminateImmediately) {
      this.useSpotInstances = useSpotInstances;
      this.useSpotBlock = useSpotBlock;
      this.useEbsVolumes = useEbsVolumes;
      this.testEbsEncryption = testEbsEncryption;
      this.terminateImmediately = terminateImmediately;
    }

    // TODO : extract AMI out to property file after extracting out Mastadon related tests/properties

    private static final Map<String, String> CENTOS64_AMI_BY_REGION = ImmutableMap.of(
        "us-west-1", "ami-5d456c18",
        "us-west-2", "ami-b3bf2f83"
    );

    private static final Map<String, String> RHEL64_AMI_BY_REGION = ImmutableMap.of(
        "us-west-1", "ami-6283a827",
        "us-west-2", "ami-b8a63b88"
    );

    private void createEC2Instance(
        AWSCredentialsProvider credentialsProvider,
        String subnetId,
        String securityGroup,
        String kmsKeyArn,
        String region) throws InterruptedException {

      boolean success = true;

      String ami;
      String spotBidUSDPerHour = null;
      String spotString = "";
      Integer blockDurationMinutes = null;

      if (useSpotInstances) {
        ami = CENTOS64_AMI_BY_REGION.get(region); //CentOS 6.4 PV
        spotBidUSDPerHour = "0.50";
        spotString = "Spot ";
        if (useSpotBlock) {
          blockDurationMinutes = DEFAULT_BLOCK_DURATION_MINUTES;
        }
      } else {
        ami = RHEL64_AMI_BY_REGION.get(region); //RHEL 6.4 PV
      }

      String username = System.getProperty("user.name");

      // Configure provider
      LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
      putConfig(providerConfigMap, REGION, region);

      // Create provider
      AmazonEC2ClientSpyProvider clientSpyProvider =
          new AmazonEC2ClientSpyProvider(credentialsProvider,
              new ClientConfigurationFactory().getConfig());
      EC2Provider ec2Provider = getEc2Provider(new SimpleConfiguration(providerConfigMap),
                                               credentialsProvider, clientSpyProvider);
      CustomTagMappings customTagMappings = ec2Provider.getEC2TagHelper().getCustomTagMappings();

      // mock eventual consistency when waiting for instances to start
      // NOTE: client spy must be retrieved after getEc2Provider is called
      AmazonEC2AsyncClient ec2AsyncClientSpy = clientSpyProvider.getClientSpy();
      final AmazonServiceException eventualConsistencyException =
          new AmazonServiceException("Eventual consistency exception");
      eventualConsistencyException.setErrorCode(EC2Provider.INVALID_INSTANCE_ID_NOT_FOUND);
      Answer<Object> describeInstanceStatusAnswer = new CallsRealMethods() {
        private Map<String, Integer> instancesTimesSeen = Maps.newHashMap();

        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          // simulate eventual consistency failure the first time we see each instance
          int minTimesSeen = Integer.MAX_VALUE;
          for (String instanceId : invocationOnMock
              .getArgumentAt(0, DescribeInstanceStatusRequest.class).getInstanceIds()) {
            Integer timesSeen = instancesTimesSeen.get(instanceId);
            if (timesSeen == null) {
              timesSeen = 0;
            }
            minTimesSeen = Math.min(minTimesSeen, timesSeen);
            instancesTimesSeen.put(instanceId, timesSeen + 1);
          }
          if (minTimesSeen == 0 || minTimesSeen == 2) {
            LOG.info("Simulating eventual consistency exception. Instances seen " + minTimesSeen +
                " times. " + invocationOnMock);
            throw eventualConsistencyException;
          }

          DescribeInstanceStatusResult result =
              (DescribeInstanceStatusResult) super.answer(invocationOnMock);
          if (minTimesSeen == 1) {
            for (com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceStatus
                instanceStatus : result.getInstanceStatuses()) {
              com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceState
                  instanceState = instanceStatus.getInstanceState();
              instanceState.setCode(0);
              instanceState.setName(com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceStateName.Pending);
            }
            LOG.info("Instances seen " + minTimesSeen + " time. Modified result = " + result);
          }
          return result;
        }
      };
      doAnswer(describeInstanceStatusAnswer)
          .when(ec2AsyncClientSpy).describeInstanceStatus(any(DescribeInstanceStatusRequest.class));

      // Configure instance template
      Map<String, String> instanceTemplateConfigMap = new LinkedHashMap<>();
      String templateName = username + "-test";
      putConfig(instanceTemplateConfigMap, INSTANCE_NAME_PREFIX, templateName);
      putConfig(instanceTemplateConfigMap, IMAGE, ami);
      putConfig(instanceTemplateConfigMap, SECURITY_GROUP_IDS, securityGroup);
      putConfig(instanceTemplateConfigMap, SUBNET_ID, subnetId);
      putConfig(instanceTemplateConfigMap, TYPE, "m3.medium");
      putConfig(instanceTemplateConfigMap, USE_SPOT_INSTANCES, String.valueOf(useSpotInstances));
      putConfig(instanceTemplateConfigMap, SPOT_BID_USD_PER_HR, spotBidUSDPerHour);
      putConfig(instanceTemplateConfigMap, BLOCK_DURATION_MINUTES, blockDurationMinutes == null ?
          null : blockDurationMinutes.toString());
      putConfig(instanceTemplateConfigMap, USER_DATA, TEST_USER_DATA);
      Map<String, String> instanceTemplateTags = new LinkedHashMap<>();
      instanceTemplateTags.put(InstanceTags.OWNER.getTagKey(), username);
      instanceTemplateTags.put(EXTRA_TAG, "foo");

      int ebsVolumeCount = 0;
      int ebsVolumeSizeGiB = 500;
      String ebsVolumeType = "st1";
      String enableEbsEncryption = "true";

      Set<String> mountedEbsVolumes = Sets.newHashSet();
      if (useEbsVolumes) {
        if (terminateImmediately) {
          ebsVolumeSizeGiB = 2000;
          ebsVolumeCount = 25;
        } else {
          ebsVolumeCount = 2;
        }
        putConfig(instanceTemplateConfigMap, EBS_VOLUME_COUNT, String.valueOf(ebsVolumeCount));
        putConfig(instanceTemplateConfigMap, EBS_VOLUME_SIZE_GIB, String.valueOf(ebsVolumeSizeGiB));
        putConfig(instanceTemplateConfigMap, EBS_VOLUME_TYPE, ebsVolumeType);

        if (testEbsEncryption) {
          putConfig(instanceTemplateConfigMap, ENCRYPT_ADDITIONAL_EBS_VOLUMES, enableEbsEncryption);
          putConfig(instanceTemplateConfigMap, EBS_KMS_KEY_ID, kmsKeyArn);
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

          try {
            InstanceAttribute instanceAttribute = ec2Provider.describeInstanceAttribute(
                instance.unwrap().getInstanceId(), InstanceAttributeName.UserData);
            assertEquals(TEST_USER_DATA, instanceAttribute.getUserData());
          } catch (AmazonServiceException e) {
            LOG.warning("Cannot verify user data.");
          }
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
          if (!terminateImmediately) {
            if (instanceStatus != InstanceStatus.RUNNING) {
              success = false;
              break;
            }
          } else {
            if (instanceStatus == InstanceStatus.RUNNING) {
              success = false;
              break;
            }
          }
        }

        if (useSpotInstances) {
          verifySpotInstance(ec2Provider.getClient(), instances);
        }

        if (useEbsVolumes && success && !terminateImmediately) {
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
                  assertTrue(volume.getKmsKeyId().equals(kmsKeyArn));
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
                break;
              }
            }

            if (rootVolumeCount != 1) {
              LOG.severe("Found more than one volume with root volume type");
              success = false;
              break;
            }

            if (mountedVolumeCount != ebsVolumeCount) {
              LOG.severe("Incorrect number of ebs volumes");
              success = false;
              break;
            }
          }
        }
      } finally {
        // Terminate instances
        ec2Provider.delete(instanceTemplate, virtualInstanceIds);

        // Wait for instances to be deleted
        LOG.info("Waiting for " + spotString + "instances to be deleted.");
        Map<String, InstanceState> instanceStates = waitForInstanceStates(
            ec2Provider,
            instanceTemplate,
            virtualInstanceIds,
            EnumSet.of(InstanceStatus.DELETED, InstanceStatus.UNKNOWN),
            EnumSet.of(InstanceStatus.FAILED, InstanceStatus.STOPPED)
        );

        if (success) {
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

    @Test
    public void testCreateEC2Instance() throws InterruptedException {
      assumeTrue(isLive());
      assumeFalse(terminateImmediately);
      assumeNotNull(testSubnet);
      assumeNotNull(testSecurityGroup);
      assumeNotNull(testKmsKeyArn);
      assumeNotNull(testRegion);

      // Configure credentials
      Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
      AWSCredentialsProvider credentialsProvider =
          new AWSCredentialsProviderChainProvider()
              .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      createEC2Instance(credentialsProvider,
          testSubnet,
          testSecurityGroup,
          testKmsKeyArn,
          testRegion);
    }

    @Test
    public void testDelegatedRoleAccessInMastodonTest() throws InterruptedException {
      assumeTrue(isLive());
      assumeFalse(terminateImmediately);
      assumeNotNull(delegatedRoleArn);
      assumeNotNull(delegatedRoleExternalId);
      assumeNotNull(externalAccountSubnetId);
      assumeNotNull(externalAccountSecurityGroup);
      assumeNotNull(externalAccountKmsKeyArn);
      assumeNotNull(externalAccountRegion);

      LOG.info("Testing delegated role access in mastodon-test account.");
      // Configure credentials
      Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
      credentialsConfigMap.put("delegatedRoleArn", delegatedRoleArn);
      credentialsConfigMap.put("delegatedRoleExternalId", delegatedRoleExternalId);
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);

      // When creating AWSSecurityTokenServiceClient, it uses the default credential
      // provider chain, DefaultAWSCredentialsProviderChain, which means when the live
      // tests run it needs to get the AWS credentials from environment variables,
      // or Java system properties, or credential profiles file, or ec2 instance profile.
      // In our Jenkins server, it injects the AWS credentials as environment variables.
      AWSCredentialsProvider credentialsProvider =
          new AWSCredentialsProviderChainProvider()
              .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      createEC2Instance(credentialsProvider,
          externalAccountSubnetId,
          externalAccountSecurityGroup,
          externalAccountKmsKeyArn,
          externalAccountRegion);
    }

    @Test(expected = UnrecoverableProviderException.class)
    public void testCreateEC2TerminatedImmediately() throws InterruptedException {
      assumeTrue(isLive());
      assumeTrue(terminateImmediately);
      assumeNotNull(testSubnet);
      assumeNotNull(testSecurityGroup);
      assumeNotNull(testKmsKeyArn);
      assumeNotNull(testRegion);

      LOG.info("Testing EC2 provider handling of immediate termination.");

      // Configure credentials
      Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
      SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
      AWSCredentialsProvider credentialsProvider =
          new AWSCredentialsProviderChainProvider()
              .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

      createEC2Instance(credentialsProvider,
          testSubnet,
          testSecurityGroup,
          testKmsKeyArn,
          testRegion);
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

    private void verifySpotInstance(AmazonEC2Client client, Collection<EC2Instance> instances) {
      DescribeSpotInstanceRequestsRequest request = new DescribeSpotInstanceRequestsRequest();

      LOG.info("Verifying each instance is spot");
      for (EC2Instance instance : instances) {
        assertTrue(Boolean.parseBoolean(
            instance.getProperties()
                .get(EC2Instance.EC2InstanceDisplayPropertyToken.SPOT.unwrap().getDisplayKey())));

        String spotRequestId = instance.unwrap().getSpotInstanceRequestId();
        request.withSpotInstanceRequestIds(spotRequestId);
      }
      LOG.info("Done verify that each instance is spot");

      if (useSpotBlock && request.getSpotInstanceRequestIds().size() > 0) {
        LOG.info("Verifying blockDurationMinutes");
        DescribeSpotInstanceRequestsResult results = client.describeSpotInstanceRequests(request);
        for (SpotInstanceRequest spotRequest : results.getSpotInstanceRequests()) {
          LOG.info("Spot request = " + spotRequest);
          assertTrue(DEFAULT_BLOCK_DURATION_MINUTES.equals(spotRequest.getBlockDurationMinutes()));
        }
        LOG.info("Done verifying blockDurationMinutes");
      }
    }
  }

  private static class AmazonEC2ClientSpyProvider extends AmazonEC2ClientProvider {
    private AmazonEC2AsyncClient clientSpy = null;

    public AmazonEC2ClientSpyProvider(AWSCredentialsProvider awsCredentialsProvider,
        ClientConfiguration clientConfiguration) {
      super(awsCredentialsProvider, clientConfiguration);
    }

    public synchronized AmazonEC2AsyncClient getClientSpy() {
      if (clientSpy == null) {
        throw new IllegalStateException(
            "clientSpy has not be set yet. getClient must be called before getting the clientSpy");
      }
      return clientSpy;
    }

    @Override
    public synchronized AmazonEC2AsyncClient getClient(Configured configuration,
        PluginExceptionConditionAccumulator accumulator,
        LocalizationContext providerLocalizationContext,
        boolean verify) {
      clientSpy = spy(
          super.getClient(configuration, accumulator, providerLocalizationContext, verify));
      return clientSpy;
    }
  }
}
