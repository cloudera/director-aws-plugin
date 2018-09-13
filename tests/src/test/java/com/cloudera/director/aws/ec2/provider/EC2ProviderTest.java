// (c) Copyright 2016 Cloudera, Inc.
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

import static com.cloudera.director.spi.v2.provider.Launcher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.aws.clientprovider.ClientProvider;
import com.cloudera.director.aws.ec2.EphemeralDeviceMappings;
import com.cloudera.director.aws.ec2.VirtualizationMappings;
import com.cloudera.director.aws.ec2.allocation.IdType;
import com.cloudera.director.aws.ec2.ebs.EBSDeviceMappings;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Instance;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceState;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceStateName;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceStatus;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Reservation;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.StateReason;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Tag;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.kms.AWSKMSClient;
import com.cloudera.director.aws.shaded.com.google.common.base.Function;
import com.cloudera.director.aws.shaded.com.google.common.base.Predicate;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.aws.shaded.org.joda.time.DateTime;
import com.cloudera.director.aws.shaded.org.joda.time.Duration;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class EC2ProviderTest {

  private static final Logger LOG = Logger.getLogger(EC2ProviderTest.class.getName());

  private static void putConfig(Map<String, String> configMap, ConfigurationPropertyToken propertyToken,
      String value) {
    if (value != null) {
      configMap.put(propertyToken.unwrap().getConfigKey(), value);
    }
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private EC2Provider ec2Provider;
  private AmazonEC2AsyncClient ec2Client;
  private AmazonAutoScalingAsyncClient autoScalingClient;
  private AmazonIdentityManagementClient identityManagementClient;
  private AWSKMSClient kmsClient;

  private BlockDeviceMapping ebs1, ebs1x, ebs2;
  private BlockDeviceMapping eph2;

  @Before
  public void setUp() {
    setupEC2Provider();
    setupBlockDeviceMappings();
  }

  @SuppressWarnings("unchecked")
  private void setupEC2Provider() {
    // Configure ephemeral device mappings
    EphemeralDeviceMappings ephemeralDeviceMappings =
        EphemeralDeviceMappings.getTestInstance(ImmutableMap.of("m3.medium", 1),
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
    CustomTagMappings customTagMappings = new CustomTagMappings(ConfigFactory.empty());

    ec2Client = mock(AmazonEC2AsyncClient.class);
    ClientProvider<AmazonEC2AsyncClient> ec2ClientProvider = mock(ClientProvider.class);
    when(ec2ClientProvider.getClient(any(Configured.class),
          any(PluginExceptionConditionAccumulator.class), any(LocalizationContext.class),
          anyBoolean())).thenReturn(ec2Client);
    autoScalingClient = mock(AmazonAutoScalingAsyncClient.class);
    ClientProvider<AmazonAutoScalingAsyncClient> autoScalingClientProvider = mock(ClientProvider.class);
    when(autoScalingClientProvider.getClient(any(Configured.class),
        any(PluginExceptionConditionAccumulator.class), any(LocalizationContext.class),
        anyBoolean())).thenReturn(autoScalingClient);
    ClientProvider identityManagementClientProvider = mock(ClientProvider.class);
    when(identityManagementClientProvider.getClient(any(Configured.class),
        any(PluginExceptionConditionAccumulator.class), any(LocalizationContext.class),
        anyBoolean())).thenReturn(identityManagementClient);
    ClientProvider kmsClientProvider = mock(ClientProvider.class);
    when(identityManagementClientProvider.getClient(any(Configured.class),
        any(PluginExceptionConditionAccumulator.class), any(LocalizationContext.class),
        anyBoolean())).thenReturn(kmsClient);
    ec2Provider = new EC2Provider(
        new SimpleConfiguration(),
        ephemeralDeviceMappings,
        ebsDeviceMappings,
        ebsMetadata,
        virtualizationMappings,
        awsFilters,
        awsTimeouts,
        customTagMappings,
        NetworkRules.EMPTY_RULES,
        ec2ClientProvider,
        autoScalingClientProvider,
        identityManagementClientProvider,
        kmsClientProvider,
        true,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

  }

  private void setupBlockDeviceMappings() {
    ebs1 = new BlockDeviceMapping()
        .withDeviceName("/dev/sda1")
        .withEbs(new EbsBlockDevice()
            .withVolumeSize(10)
            .withVolumeType("gp2")
            .withSnapshotId("snap-1"));
    ebs1x = new BlockDeviceMapping()
        .withDeviceName("/dev/sda")
        .withEbs(new EbsBlockDevice()
            .withVolumeSize(10)
            .withVolumeType("gp2")
            .withSnapshotId("snap-1"));
    ebs2 = new BlockDeviceMapping()
        .withDeviceName("/dev/sdb1")
        .withEbs(new EbsBlockDevice()
            .withVolumeSize(20)
            .withVolumeType("gp2")
            .withSnapshotId("snap-2"));

    eph2 = new BlockDeviceMapping()
        .withDeviceName("/dev/sdb")
        .withVirtualName("ephemeral0");
  }

  @Test
  public void testExactMatch() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(ebs2, ebs1, eph2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isEqualTo(ebs1);
  }

  @Test
  public void testBestMatch() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(ebs2, ebs1x, eph2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isEqualTo(ebs1x);
  }

  @Test
  public void testFirstEbs() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(eph2, ebs2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isEqualTo(ebs2);
  }

  @Test
  public void testNoRootDevice() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(eph2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isNull();
  }

  @Test
  public void testForEachInstanceByVirtualInstanceId() throws Exception {
    // This test verifies that forEachInstance returns at most one instance per specified
    // virtual instance id. Also, forEachInstance should prefer running or pending instances
    // over shutting-down or terminated instances.

    final List<Instance> handledInstances = Lists.newArrayList();
    Function<Instance, Void> instanceHandler =
        instance -> { handledInstances.add(instance); return null; };

    List<String> virtualInstanceIds = ImmutableList.of("vid-0001", "vid-0002", "vid-0003");

    String virtualInstanceTag = ec2Provider.ec2TagHelper.getClouderaDirectorIdTagName();

    Instance[] expectedInstances = {
        // running takes precedence
        new Instance().withInstanceId("i-0002")
            .withTags(new Tag(virtualInstanceTag, "vid-0001"))
            .withState(new InstanceState().withName("running")),
        // pending takes precedence
        new Instance().withInstanceId("i-0005")
            .withTags(new Tag(virtualInstanceTag, "vid-0002"))
            .withState(new InstanceState().withName("pending")),
        // terminated is ok if there are no other instances with the same virtual instance id
        new Instance().withInstanceId("i-0006")
            .withTags(new Tag(virtualInstanceTag, "vid-0003"))
            .withState(new InstanceState().withName("terminated"))
    };
    List<Instance> instances = ImmutableList.of(
        new Instance().withInstanceId("i-0001")
            .withTags(new Tag(virtualInstanceTag, "vid-0001"))
            .withState(new InstanceState().withName("shutting-down")),
        expectedInstances[0],
        new Instance().withInstanceId("i-0003")
            .withTags(new Tag(virtualInstanceTag, "vid-0001"))
            .withState(new InstanceState().withName("terminated")),
        new Instance().withInstanceId("i-0004")
            .withTags(new Tag(virtualInstanceTag, "vid-0002"))
            .withState(new InstanceState().withName("terminated")),
        expectedInstances[1],
        expectedInstances[2]
    );
    LOG.info("instances = " + instances);

    DescribeInstancesResult describeInstancesResult = new DescribeInstancesResult()
        .withReservations(new Reservation().withInstances(instances));

    when(ec2Client.describeInstances(any(DescribeInstancesRequest.class)))
        .thenReturn(describeInstancesResult);

    ec2Provider.allocationHelper.forEachInstance(virtualInstanceIds, instanceHandler,
        IdType.VIRTUAL_INSTANCE_ID);

    assertThat(handledInstances.size()).isEqualTo(virtualInstanceIds.size());
    assertThat(handledInstances).containsOnlyOnce(expectedInstances);
  }

  @Test
  public void testForEachInstanceByResult() throws Exception {
    final List<Instance> handledInstances = Lists.newArrayList();
    Function<Instance, Void> instanceHandler =
        instance -> { handledInstances.add(instance); return null; };

    List<Instance> firstInstances = ImmutableList.of(
        new Instance().withInstanceId("i-0001"),
        new Instance().withInstanceId("i-0002")
    );

    DescribeInstancesResult firstResult = new DescribeInstancesResult()
        .withReservations(new Reservation().withInstances(firstInstances))
        .withNextToken("next");

    List<Instance> secondInstances = ImmutableList.of(
        new Instance().withInstanceId("i-0003"),
        new Instance().withInstanceId("i-0004")
    );

    DescribeInstancesResult secondResult = new DescribeInstancesResult()
        .withReservations(new Reservation().withInstances(secondInstances));

    when(ec2Client.describeInstances(argThat(matchesNextToken("next")))).thenReturn(secondResult);

    ec2Provider.allocationHelper.forEachInstance(firstResult, instanceHandler);

    assertThat(handledInstances).containsAll(firstInstances);
    assertThat(handledInstances).containsAll(secondInstances);
    verify(ec2Client).describeInstances(argThat(matchesNextToken("next")));
  }

  @Test(timeout=1000L)
  public void testWaitUntilInstanceHasStartedSuccess() throws Exception {
    String ec2InstanceId = "i-test";

    InstanceStateName instanceStateName = InstanceStateName.Running;
    DescribeInstanceStatusResult instanceStatusResult = new DescribeInstanceStatusResult()
        .withInstanceStatuses(new InstanceStatus()
            .withInstanceId(ec2InstanceId)
            .withInstanceState(new InstanceState().withName(instanceStateName.toString())));
    when(ec2Client.describeInstanceStatus(any(DescribeInstanceStatusRequest.class)))
        .thenReturn(instanceStatusResult);

    boolean result = ec2Provider.allocationHelper.waitUntilInstanceHasStarted(
        ec2InstanceId, DateTime.now().plus(1000L));
    assertThat(result).isTrue();
  }

  @Test(timeout=1000L)
  public void testWaitUntilInstanceHasStartedFailed() throws Exception {
    String ec2InstanceId = "i-test";

    String failureReason = "failure reason";
    List<InstanceStateName> failedStates = ImmutableList.of(InstanceStateName.Terminated,
        InstanceStateName.ShuttingDown);
    for (InstanceStateName instanceStateName : failedStates) {
      DescribeInstanceStatusResult instanceStatusResult = new DescribeInstanceStatusResult()
          .withInstanceStatuses(new InstanceStatus()
              .withInstanceId(ec2InstanceId)
              .withInstanceState(new InstanceState().withName(instanceStateName.toString())));
      when(ec2Client.describeInstanceStatus(any(DescribeInstanceStatusRequest.class)))
          .thenReturn(instanceStatusResult);

      DescribeInstancesResult failureReasonResult = new DescribeInstancesResult()
          .withReservations(new Reservation()
              .withInstances(new Instance()
                  .withStateReason(new StateReason()
                .withMessage(failureReason))));
      when(ec2Client.describeInstances(any(DescribeInstancesRequest.class)))
          .thenReturn(failureReasonResult);

      boolean result = ec2Provider.allocationHelper.waitUntilInstanceHasStarted(
          ec2InstanceId, DateTime.now().plus(1000L));
      assertThat(result).isFalse();
    }
  }

  @Test(timeout=10000L)
  public void testWaitUntilInstanceHasStartedTimeout() throws Exception {
    String ec2InstanceId = "i-test";

    InstanceStateName instanceStateName = InstanceStateName.Pending;
    DescribeInstanceStatusResult instanceStatusResult = new DescribeInstanceStatusResult()
        .withInstanceStatuses(new InstanceStatus()
            .withInstanceId(ec2InstanceId)
            .withInstanceState(new InstanceState().withName(instanceStateName.toString())));
    when(ec2Client.describeInstanceStatus(any(DescribeInstanceStatusRequest.class)))
        .thenReturn(instanceStatusResult);

    expectedException.expect(TimeoutException.class);
    ec2Provider.allocationHelper.waitUntilInstanceHasStarted(
        ec2InstanceId, DateTime.now().plus(1000L));
  }

  @Test
  public void testFind() throws Exception {
    Instance instance1 = new Instance()
        .withInstanceId("id1")
        .withTags(new Tag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "vid1"))
        .withState(new InstanceState().withName(InstanceStateName.Pending));
    Instance instance1p = new Instance()
        .withInstanceId("id1")
        .withTags(new Tag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "vid1"))
        .withState(new InstanceState().withName(InstanceStateName.Running));
    Instance instance2 = new Instance()
        .withInstanceId("id2")
        .withTags(new Tag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "vid2"))
        .withState(new InstanceState().withName(InstanceStateName.Pending));
    Map<String, Instance> vidToIds = Maps.newHashMap();
    vidToIds.put("vid1", instance1p);

    when(ec2Client.describeInstances(any(DescribeInstancesRequest.class)))
        .thenReturn(
            new DescribeInstancesResult().withReservations(new Reservation()))
        .thenReturn(
            new DescribeInstancesResult().withReservations(new Reservation().withInstances(instance1, instance2)))
        .thenReturn(
            new DescribeInstancesResult().withReservations(new Reservation().withInstances(instance1p, instance2)));

    List<Map.Entry<String, Instance>> vidToInstances = Lists.newArrayList(ec2Provider.allocationHelper.doFind(
        vidToIds.keySet(),
        null,
        new Predicate<Instance>() {
          @Override
          public boolean apply(Instance instance) {
            boolean result = InstanceStateName.Running.toString().equals(instance.getState().getName());
            return result;
          }
        },
        Duration.millis(3000l)));

    assertThat(vidToInstances.size()).isEqualTo(vidToIds.size());

    for (Map.Entry<String, Instance> vidToInstance : vidToInstances) {
      assertThat(vidToIds.containsKey(vidToInstance.getKey())).isTrue();
      assertThat(Objects.equals(vidToInstance.getValue(), vidToIds.get(vidToInstance.getKey()))).isTrue();
    }
  }

  @Test
  public void testFindNoWait() throws Exception {
    Instance instance1 = new Instance()
        .withInstanceId("id1")
        .withTags(new Tag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "vid1"))
        .withState(new InstanceState().withName(InstanceStateName.Pending));
    Instance instance1p = new Instance()
        .withInstanceId("id1")
        .withTags(new Tag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "vid1"))
        .withState(new InstanceState().withName(InstanceStateName.Running));
    Instance instance2 = new Instance()
        .withInstanceId("id2")
        .withTags(new Tag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "vid2"))
        .withState(new InstanceState().withName(InstanceStateName.Pending));
    Map<String, Instance> vidToIds = Maps.newHashMap();

    when(ec2Client.describeInstances(any(DescribeInstancesRequest.class)))
        .thenReturn(
            new DescribeInstancesResult().withReservations(new Reservation()))
        .thenReturn(
            new DescribeInstancesResult().withReservations(new Reservation().withInstances(instance1, instance2)))
        .thenReturn(
            new DescribeInstancesResult().withReservations(new Reservation().withInstances(instance1p, instance2)));

    List<Map.Entry<String, Instance>> vidToInstances = Lists.newArrayList(ec2Provider.allocationHelper.doFind(
        vidToIds.keySet(),
        null,
        new Predicate<Instance>() {
          @Override
          public boolean apply(Instance instance) {
            boolean result = InstanceStateName.Running.toString().equals(instance.getState().getName());
            return result;
          }
        },
        Duration.ZERO));

    assertThat(vidToInstances.isEmpty()).isTrue();
  }

  @Test
  public void testFindEmpty() throws Exception {
    List<Map.Entry<String, Instance>> vidToInstances = Lists.newArrayList(ec2Provider.allocationHelper.doFind(
        Collections.<String>emptyList(),
        null,
        new Predicate<Instance>() {
          @Override
          public boolean apply(Instance instance) {
            return InstanceStateName.Running.name().equals(instance.getState().getName());
          }
        },
        Duration.millis(5000l)));
    assertThat(vidToInstances.isEmpty()).isTrue();
  }

  @Test
  public void testDeleteEmpty() throws Exception {
    ec2Provider.delete(null, Collections.EMPTY_LIST);
  }

  public static DescribeInstancesRequestTokenMatcher matchesNextToken(String nextToken) {
    return new DescribeInstancesRequestTokenMatcher(nextToken);
  }

  public static class DescribeInstancesRequestTokenMatcher extends TypeSafeMatcher<DescribeInstancesRequest> {
    private final String nextToken;

    public DescribeInstancesRequestTokenMatcher(String nextToken) {
      this.nextToken = requireNonNull(nextToken, "nextToken is null");
    }

    @Override
    protected boolean matchesSafely(DescribeInstancesRequest describeInstancesRequest) {
      return nextToken.equals(describeInstancesRequest.getNextToken());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("nextToken should be ").appendText(nextToken);
    }
  }
}
