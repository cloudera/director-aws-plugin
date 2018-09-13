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

package com.cloudera.director.aws.ec2.provider;

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
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USER_DATA_UNENCODED;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;
import static com.cloudera.director.aws.ec2.common.EC2Exceptions.INVALID_INSTANCE_ID_NOT_FOUND;
import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.Ami;
import static com.cloudera.director.aws.test.EC2ProviderCommon.CENTOS64_AMI_BY_REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.EXTRA_TAG;
import static com.cloudera.director.aws.test.EC2ProviderCommon.RHEL67_AMI_BY_REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.getEc2Provider;
import static com.cloudera.director.aws.test.EC2ProviderCommon.isLive;
import static com.cloudera.director.aws.test.EC2ProviderCommon.putConfig;
import static com.cloudera.director.aws.test.EC2ProviderCommon.waitForInstanceStates;
import static com.cloudera.director.aws.test.EC2ProviderCommon.waitUntilRunningOrTerminal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.STSAssumeNRolesSessionCredentialsProvider.RoleConfiguration;
import com.cloudera.director.aws.clientprovider.AmazonEC2ClientProvider;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
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
import com.cloudera.director.aws.test.LiveTestProperties;
import com.cloudera.director.aws.test.TestInstanceTemplate;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
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

import org.assertj.core.util.Lists;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.util.Base64Utils;

/**
 * Tests EC2Provider with various combination of parameters.
 */
@RunWith(Parameterized.class)
public class EC2ProviderParameterizedLiveTest {

  private static final Logger LOG = Logger.getLogger(EC2ProviderParameterizedLiveTest.class.getName());

  private static final String VOLUME_NOT_FOUND_ERR_CODE = "InvalidVolume.NotFound";

  private static final Integer DEFAULT_BLOCK_DURATION_MINUTES = 60;

  private static final String TEST_USER_DATA = "Test user data #&*^@#$(*!@";
  private static final String ENCODED_TEST_USER_DATA = Base64Utils.encodeToString(TEST_USER_DATA.getBytes());

  private static String testRegion;
  private static String testSubnet;
  private static String testSecurityGroup;
  private static String testKmsKeyArn;
  private static List<RoleConfiguration> delegatedRoleConfigurations;
  private static String externalAccountRegion;
  private static String externalAccountSubnetId;
  private static String externalAccountSecurityGroup;
  private static String externalAccountKmsKeyArn;

  @BeforeClass
  public static void setup() {
    if (isLive()) {
      LiveTestProperties liveProps = LiveTestProperties.loadLiveTestProperties();

      testRegion = liveProps.getTestRegion();
      testSubnet = liveProps.getTestSubnet();
      testSecurityGroup = liveProps.getTestSecurityGroup();
      testKmsKeyArn = liveProps.getTestKmsKeyArn();

      delegatedRoleConfigurations = liveProps.getDelegatedRoleConfigurations();
      externalAccountRegion = liveProps.getExternalAccountRegion();
      externalAccountSubnetId = liveProps.getExternalAccountSubnetId();
      externalAccountSecurityGroup = liveProps.getExternalAccountSecurityGroup();
      externalAccountKmsKeyArn = liveProps.getExternalAccountKmsKeyArn();
    }
  }

  private enum UserDataParameter {
    UNENCODED,
    ENCODED,
    INVALID;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {false, false, false, false, false, UserDataParameter.UNENCODED}, // on-demand instances
        {false, false, false, false, false, UserDataParameter.INVALID},  // on-demand instances + user data failure
        {false, false, true, false, false, UserDataParameter.ENCODED},  // on-demand instances + ebs volumes + encoded user data
        {false, false, true, true, false, UserDataParameter.UNENCODED},   // on-demand instances + ebs volumes + ebs encryption
        {true, false, false, false, false, UserDataParameter.UNENCODED},  // spot instances
        {true, true, false, false, false, UserDataParameter.UNENCODED},   // spot instances with block duration
        {true, false, true, false, false, UserDataParameter.UNENCODED},   // spot instances + ebs volumes
        {false, false, false, false, true, UserDataParameter.UNENCODED}   // on-demand instances + delegation
    });
  }

  private boolean useSpotInstances;
  private boolean useSpotBlock;
  private boolean useEbsVolumes;
  private boolean testEbsEncryption;
  private boolean testDelegation;
  private UserDataParameter userDataParameter;

  public EC2ProviderParameterizedLiveTest(boolean useSpotInstances, boolean useSpotBlock,
                           boolean useEbsVolumes, boolean testEbsEncryption,
                           boolean testDelegation, UserDataParameter userDataParameter) {
    this.useSpotInstances = useSpotInstances;
    this.useSpotBlock = useSpotBlock;
    this.useEbsVolumes = useEbsVolumes;
    this.testEbsEncryption = testEbsEncryption;
    this.testDelegation = testDelegation;
    this.userDataParameter = userDataParameter;
  }

  private void createEC2Instance(
      AWSCredentialsProvider credentialsProvider,
      String subnetId,
      String securityGroup,
      String kmsKeyArn,
      String region) throws InterruptedException {

    boolean success = true;

    Ami ami;
    String amiId;
    String spotBidUSDPerHour = null;
    String spotString = "";
    Integer blockDurationMinutes = null;

    if (useSpotInstances) {
      ami = CENTOS64_AMI_BY_REGION.get(region);
      amiId = ami.getId();
      spotBidUSDPerHour = "0.50";
      spotString = "Spot ";
      if (useSpotBlock) {
        blockDurationMinutes = DEFAULT_BLOCK_DURATION_MINUTES;
      }
    } else {
      ami = RHEL67_AMI_BY_REGION.get(region);
      amiId = ami.getId();
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
    eventualConsistencyException.setErrorCode(INVALID_INSTANCE_ID_NOT_FOUND);
    Answer<Object> describeInstanceStatusAnswer = new CallsRealMethods() {
      private Map<String, Integer> instancesTimesSeen = Maps.newHashMap();

      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        // simulate eventual consistency failure the first time we see each instance
        int minTimesSeen = Integer.MAX_VALUE;
        DescribeInstanceStatusRequest request = (DescribeInstanceStatusRequest) invocationOnMock.getArgument(0);
        for (String instanceId : request.getInstanceIds()) {
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
    TestInstanceTemplate template = new TestInstanceTemplate();
    template.addConfig(IMAGE, amiId);
    template.addConfig(SECURITY_GROUP_IDS, securityGroup);
    template.addConfig(SUBNET_ID, subnetId);
    template.addConfig(TYPE, "m3.medium");
    template.addConfig(USE_SPOT_INSTANCES, String.valueOf(useSpotInstances));
    template.addConfig(SPOT_BID_USD_PER_HR, spotBidUSDPerHour);
    template.addConfig(BLOCK_DURATION_MINUTES, blockDurationMinutes == null ?
        null : blockDurationMinutes.toString());
    switch (userDataParameter) {
      case UNENCODED:
        template.addConfig(USER_DATA_UNENCODED, TEST_USER_DATA);
        break;
      case ENCODED:
        template.addConfig(USER_DATA, ENCODED_TEST_USER_DATA);
        break;
      case INVALID:
        template.addConfig(USER_DATA, TEST_USER_DATA); // should be base64 encoded, ergo invalid
        break;
    }
    template.addTag(EXTRA_TAG, "foo");

    int ebsVolumeCount = 0;
    int ebsVolumeSizeGiB = 500;
    String ebsVolumeType = "st1";
    String enableEbsEncryption = "true";

    Set<String> mountedEbsVolumes = Sets.newHashSet();
    if (useEbsVolumes) {
      ebsVolumeCount = 2;
      template.addConfig(EBS_VOLUME_COUNT, String.valueOf(ebsVolumeCount));
      template.addConfig(EBS_VOLUME_SIZE_GIB, String.valueOf(ebsVolumeSizeGiB));
      template.addConfig(EBS_VOLUME_TYPE, ebsVolumeType);

      if (testEbsEncryption) {
        template.addConfig(ENCRYPT_ADDITIONAL_EBS_VOLUMES, enableEbsEncryption);
        template.addConfig(EBS_KMS_KEY_ID, kmsKeyArn);
      }
    }

    // Create instance template
    EC2InstanceTemplate instanceTemplate = ec2Provider.createResourceTemplate(
        template.getTemplateName(), new SimpleConfiguration(template.getConfigs()), template.getTags());

    // Create instances
    List<String> virtualInstanceIds = Arrays.asList(UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    int instanceCount = virtualInstanceIds.size();

    LOG.info("Allocating " + instanceCount + " " + spotString + "instances.");

    try {
      ec2Provider.allocate(instanceTemplate, virtualInstanceIds, instanceCount);
    } catch (UnrecoverableProviderException e) {
      if (userDataParameter == UserDataParameter.INVALID) {
        LOG.info("User data failed as expected.");
        return;
      } else {
        fail("Transient exeception while allocating instances.", e);
      }
    }

    if (userDataParameter == UserDataParameter.INVALID) {
      fail("Allocation did not fail when giving invalid user data as expected.");
    }

    Collection<EC2Instance> instances = ec2Provider.find(instanceTemplate, virtualInstanceIds);

    try {
      for (EC2Instance instance : instances) {
        LOG.info("Created " + spotString + "instance: " + instance.getProperties());

        try {
          InstanceAttribute instanceAttribute = ec2Provider.describeInstanceAttribute(
              instance.unwrap().getInstanceId(), InstanceAttributeName.UserData);
          assertThat(TEST_USER_DATA.getBytes()).isEqualTo(
              Base64Utils.decodeFromString(instanceAttribute.getUserData()));
          LOG.info("Verified user data.");
        } catch (AmazonServiceException e) {
          LOG.warning("Cannot verify user data.");
        }
      }

      // Wait for instances to be running
      LOG.info("Waiting for " + spotString + "instances to be running.");
      Map<String, InstanceState> instanceStates = waitUntilRunningOrTerminal(ec2Provider,
          instanceTemplate, virtualInstanceIds);

      for (Map.Entry<String, InstanceState> entry : instanceStates.entrySet()) {
        String virtualInstanceId = entry.getKey();
        InstanceState instanceState = entry.getValue();
        InstanceStatus instanceStatus = instanceState.getInstanceStatus();

        LOG.info(spotString + "Instance: " + virtualInstanceId + " has status: " + instanceStatus);
        if (instanceStatus != InstanceStatus.RUNNING) {
          LOG.severe(spotString + "Instance: " + virtualInstanceId + " has status: " +
              instanceStatus + " instead of RUNNING");
          success = false;
          break;
        }
      }

      if (ami.isSupportsHostKeyFingerprintRetrieval()) {
        checkHostKeyFingerprints(ec2Provider, instanceTemplate, virtualInstanceIds);
      }

      if (useSpotInstances) {
        verifySpotInstance(ec2Provider.getClient(), instances);
      }

      if (useEbsVolumes && success) {
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
              for (Map.Entry<String, String> entry : template.getTags().entrySet()) {
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

  private static void checkHostKeyFingerprints(EC2Provider ec2Provider, EC2InstanceTemplate template,
                                               List<String> virtualInstanceIds) throws InterruptedException {
    LOG.info("Checking host key fingerprints");

    Map<String, Set<String>> fingerprintsById = ec2Provider.getHostKeyFingerprints(template, virtualInstanceIds);
    assertEquals(fingerprintsById.size(), virtualInstanceIds.size());

    Set<String> allHostKeyFingerprints = Sets.newHashSet();
    for (Map.Entry<String, Set<String>> entry: fingerprintsById.entrySet()) {
      String vid = entry.getKey();
      Set<String> fingerprints = entry.getValue();

      LOG.info(String.format("VID %s has host key fingerprints %s", vid, fingerprints.toString()));
      assertFalse(fingerprints.isEmpty());

      // verify that fingerprints are unique
      for (String fingerprint : fingerprints) {
        assertTrue(!fingerprint.isEmpty());
        assertTrue(allHostKeyFingerprints.add(fingerprint));
      }
    }
  }

  @Test
  public void testCreateEC2Instance() throws InterruptedException {
    assumeTrue(isLive());
    assumeFalse(testDelegation);

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
  public void testDelegatedRoleAccessTest() throws InterruptedException {
    assumeTrue(isLive());
    assumeTrue(testDelegation);
    assumeFalse(userDataParameter == UserDataParameter.INVALID);

    assumeFalse(delegatedRoleConfigurations.isEmpty());
    assumeNotNull(externalAccountSubnetId);
    assumeNotNull(externalAccountSecurityGroup);
    assumeNotNull(externalAccountKmsKeyArn);
    assumeNotNull(externalAccountRegion);

    LOG.info("Testing delegated role access in external account.");
    // Configure credentials
    RoleConfiguration lastRole = Iterables.getLast(delegatedRoleConfigurations);
    Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
    credentialsConfigMap.put("delegatedRoleArn", lastRole.getRoleArn());
    credentialsConfigMap.put("delegatedRoleExternalId", lastRole.getRoleExternalId());
    SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);

    // When creating AWSSecurityTokenServiceClient, it uses the default credential
    // provider chain, DefaultAWSCredentialsProviderChain, which means when the live
    // tests run it needs to get the AWS credentials from environment variables,
    // or Java system properties, or credential profiles file, or ec2 instance profile.
    List<RoleConfiguration> baseRoleConfigurations = Lists.newArrayList(Iterables.limit(
        delegatedRoleConfigurations,
        delegatedRoleConfigurations.size() - 1));
    AWSCredentialsProvider credentialsProvider =
        new AWSCredentialsProviderChainProvider(baseRoleConfigurations)
            .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    createEC2Instance(credentialsProvider,
        externalAccountSubnetId,
        externalAccountSecurityGroup,
        externalAccountKmsKeyArn,
        externalAccountRegion);
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
      clientSpy = spy(super.getClient(configuration, accumulator, providerLocalizationContext, verify));
      return clientSpy;
    }
  }

}
