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

package com.cloudera.director.aws.ec2;

import static com.cloudera.director.spi.v1.provider.Launcher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.common.AWSKMSClientProvider;
import com.cloudera.director.aws.common.AmazonEC2ClientProvider;
import com.cloudera.director.aws.common.AmazonIdentityManagementClientProvider;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfiguration;
import com.cloudera.director.aws.shaded.com.amazonaws.ClientConfigurationFactory;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Instance;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceState;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Reservation;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Tag;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

public class EC2ProviderTest {

  private static final Logger LOG = Logger.getLogger(EC2ProviderTest.class.getName());

  private EC2Provider ec2Provider;
  private AmazonEC2AsyncClient ec2Client;

  private BlockDeviceMapping ebs1, ebs1x, ebs2;
  private BlockDeviceMapping eph2;

  @Before
  public void setUp() {
    setupEC2Provider();
    setupBlockDeviceMappings();
  }

  private void setupEC2Provider() {
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
    CustomTagMappings customTagMappings = new CustomTagMappings(ConfigFactory.empty());
    ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();

    ec2Client = mock(AmazonEC2AsyncClient.class);
    AmazonEC2ClientProvider ec2ClientProvider = mock(AmazonEC2ClientProvider.class);
    when(ec2ClientProvider.getClient(any(Configured.class),
          any(PluginExceptionConditionAccumulator.class), any(LocalizationContext.class),
          anyBoolean())).thenReturn(ec2Client);
    ec2Provider = new EC2Provider(
        new SimpleConfiguration(),
        ephemeralDeviceMappings,
        ebsMetadata,
        virtualizationMappings,
        awsFilters,
        awsTimeouts,
        customTagMappings,
        NetworkRules.EMPTY_RULES,
        ec2ClientProvider,
        mock(AmazonIdentityManagementClientProvider.class, RETURNS_DEEP_STUBS),
        mock(AWSKMSClientProvider.class, RETURNS_DEEP_STUBS),
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
    EC2Provider.InstanceHandler instanceHandler = new EC2Provider.InstanceHandler() {
      @Override
      public void handle(Instance instance) {
        handledInstances.add(instance);
      }
    };

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

    ec2Provider.forEachInstance(virtualInstanceIds, instanceHandler);

    assertThat(handledInstances.size()).isEqualTo(virtualInstanceIds.size());
    assertThat(handledInstances).containsOnlyOnce(expectedInstances);
  }

  @Test
  public void testForEachInstanceByResult() throws Exception {
    final List<Instance> handledInstances = Lists.newArrayList();
    EC2Provider.InstanceHandler instanceHandler = new EC2Provider.InstanceHandler() {
      @Override
      public void handle(Instance instance) {
        handledInstances.add(instance);
      }
    };

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

    ec2Provider.forEachInstance(firstResult, instanceHandler);

    assertThat(handledInstances).containsAll(firstInstances);
    assertThat(handledInstances).containsAll(secondInstances);
    verify(ec2Client).describeInstances(argThat(matchesNextToken("next")));
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
