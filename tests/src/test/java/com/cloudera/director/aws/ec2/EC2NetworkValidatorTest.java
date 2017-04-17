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

package com.cloudera.director.aws.ec2;

import com.cloudera.director.aws.network.AccessType;
import com.cloudera.director.aws.network.Direction;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.shaded.com.google.common.collect.Range;
import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigParseOptions;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigSyntax;
import com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeNetworkAclsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeNetworkAclsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.IpPermission;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.IpRange;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.Ipv6Range;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.NetworkAcl;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.NetworkAclEntry;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.PortRange;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.SecurityGroup;

import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.List;
import java.util.Map;

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for EC2NetworkValidator
 */
public class EC2NetworkValidatorTest {

  private AmazonEC2Client ec2Client;
  private EC2Provider ec2Provider;
  private NetworkRules rules;
  private EC2NetworkValidator validator;
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  private final static String SECURITY_GROUP_KEY = SECURITY_GROUP_IDS.unwrap().getConfigKey();
  private final static String SUBNET_KEY = SUBNET_ID.unwrap().getConfigKey();

  private final static List<IpPermission> FULL_PERMISSION_SG_PERMISSIONS = ImmutableList.of(
      new IpPermission().withIpProtocol("-1").withIpv4Ranges(new IpRange().withCidrIp("0.0.0.0/0")),
      new IpPermission().withIpProtocol("-1").withIpv6Ranges(new Ipv6Range().withCidrIpv6("::/0")));

  private final static List<NetworkAclEntry> DEFAULT_NETWORK_ACK_ENTRIES = ImmutableList.of(
      new NetworkAclEntry().withRuleNumber(32767).withProtocol("-1").withEgress(true)
          .withCidrBlock("0.0.0.0/0").withRuleAction("deny"),
      new NetworkAclEntry().withRuleNumber(32767).withProtocol("-1").withEgress(false)
          .withCidrBlock("0.0.0.0/0").withRuleAction("deny"),
      new NetworkAclEntry().withRuleNumber(32768).withProtocol("-1").withEgress(true)
          .withCidrBlock("::/0").withRuleAction("deny"),
      new NetworkAclEntry().withRuleNumber(32768).withProtocol("-1").withEgress(false)
          .withCidrBlock("::/0").withRuleAction("deny")
  );

  @Before
  public void setup() {
    ConfigParseOptions options = ConfigParseOptions.defaults()
        .setSyntax(ConfigSyntax.CONF)
        .setAllowMissing(false);

    Config config = ConfigFactory.parseResourcesAnySyntax("test-network-rules.conf", options);
    rules = NetworkRules.fromConfig(config);

    ec2Client = mock(AmazonEC2Client.class);
    ec2Provider = mock(EC2Provider.class);
    when(ec2Provider.getClient()).thenReturn(ec2Client);
    when(ec2Provider.getNetworkRules()).thenReturn(rules);
    validator = new EC2NetworkValidator(ec2Provider);
    accumulator = new PluginExceptionConditionAccumulator();
  }

  private void testRules(List<NetworkAclEntry> networkAclEntries,
                         List<IpPermission> permissions,
                         boolean inbound) {
    // Mock configuration
    Configured configuration = mock(SimpleConfiguration.class);
    when(configuration.getConfigurationValue(any(ConfigurationPropertyToken.class), any(LocalizationContext.class))).
        thenReturn("");

    // Mock security group
    SecurityGroup securityGroup1 = mock(SecurityGroup.class);
    SecurityGroup securityGroup2 = mock(SecurityGroup.class);
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Lists.newArrayList(securityGroup1, securityGroup2));

    Collections.shuffle(permissions);
    Iterable<List<IpPermission>> permissionIt = Iterables.partition(permissions, permissions.size()/2 + 1);
    if (inbound) {
      when(securityGroup1.getIpPermissions()).thenReturn(Iterables.get(permissionIt, 0,
          Collections.<IpPermission>emptyList()));
      when(securityGroup2.getIpPermissions()).thenReturn(Iterables.get(permissionIt, 1,
          Collections.<IpPermission>emptyList()));
      when(securityGroup2.getIpPermissionsEgress()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);
    } else {
      when(securityGroup1.getIpPermissionsEgress()).thenReturn(Iterables.get(permissionIt, 0,
          Collections.<IpPermission>emptyList()));
      when(securityGroup2.getIpPermissionsEgress()).thenReturn(Iterables.get(permissionIt, 1,
          Collections.<IpPermission>emptyList()));
      when(securityGroup2.getIpPermissions()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);
    }
    when(securityGroup1.getGroupId()).thenReturn("sg-00000001");
    when(securityGroup2.getGroupId()).thenReturn("sg-00000002");

    // Mock network ACL
    NetworkAcl networkAcl = mock(NetworkAcl.class);
    DescribeNetworkAclsResult aclResult = mock(DescribeNetworkAclsResult.class);
    when(ec2Client.describeNetworkAcls(any(DescribeNetworkAclsRequest.class)))
        .thenReturn(aclResult);
    when(aclResult.getNetworkAcls()).thenReturn(Collections.singletonList(networkAcl));
    when(networkAcl.getEntries())
        .thenReturn(Lists.newArrayList(Iterables.concat(networkAclEntries, DEFAULT_NETWORK_ACK_ENTRIES)));
    when(networkAcl.getNetworkAclId()).thenReturn("acl-11111111");

    validator.validate("", configuration, accumulator, localizationContext);
  }

  private void assertResult(int subnetConditionSize, int securityGroupConditionSize) {
    Map<String, Collection<PluginExceptionCondition>> conditionsByKey =
        accumulator.getConditionsByKey();
    Assert.assertEquals(conditionsByKey.keySet(), ImmutableSet.of(SECURITY_GROUP_KEY, SUBNET_KEY));
    if (securityGroupConditionSize >= 0) {
      Assert.assertEquals(conditionsByKey.get(SECURITY_GROUP_KEY).toString(),
          securityGroupConditionSize, conditionsByKey.get(SECURITY_GROUP_KEY).size());
    }
    if (subnetConditionSize >= 0) {
      Assert.assertEquals(conditionsByKey.get(SUBNET_KEY).toString(),
          subnetConditionSize, conditionsByKey.get(SUBNET_KEY).size());
    }
  }

  @Test
  public void testInboundAllowedRulesViolationInSecurityGroup() {
    List<IpPermission> permissions = Lists.<IpPermission>newArrayList(
        new IpPermission().withIpProtocol("tcp").withFromPort(22).withToPort(32).withIpv4Ranges(
            new IpRange().withCidrIp("54.23.205.0/20")),
        new IpPermission().withIpProtocol("udp").withFromPort(12340).withToPort(12345).withIpv4Ranges(
            new IpRange().withCidrIp("189.203.0.0/12")),
        new IpPermission().withIpProtocol("tcp").withFromPort(32).withIpv4Ranges(
            new IpRange().withCidrIp("192.178.5.0/24"))
    );

    testRules(Collections.<NetworkAclEntry>emptyList(), permissions, true);
    assertResult(-1, 4);
  }

  @Test
  public void testInboundAllowedRulesFulfilledInSecurityGroup() {
    List<IpPermission> permissions = Lists.<IpPermission>newArrayList(
        new IpPermission().withIpProtocol("-1").withIpv4Ranges(
            new IpRange().withCidrIp("54.23.205.0/10")),
        new IpPermission().withIpProtocol("udp").withFromPort(12340).withToPort(12345).withIpv4Ranges(
            new IpRange().withCidrIp("189.203.0.0/10")),
        new IpPermission().withIpProtocol("tcp").withFromPort(32).withIpv4Ranges(
            new IpRange().withCidrIp("192.178.5.0/24"))
    );

    testRules(Collections.<NetworkAclEntry>emptyList(), permissions, true);
    assertResult(-1, 1);
  }

  @Test
  public void testInboundDeniedRulesViolationInSecurityGroup() {
    List<IpPermission> permissions = Lists.<IpPermission>newArrayList(
        new IpPermission().withIpProtocol("-1").withIpv4Ranges(
            new IpRange().withCidrIp("54.23.205.0/10")),
        new IpPermission().withIpProtocol("udp").withFromPort(12340).withToPort(12345).withIpv4Ranges(
            new IpRange().withCidrIp("189.203.0.0/10")),
        new IpPermission().withIpProtocol("udp").withFromPort(80).withIpv4Ranges(
            new IpRange().withCidrIp("88.49.0.0/20")),
        new IpPermission().withIpProtocol("tcp").withFromPort(32).withIpv4Ranges(
            new IpRange().withCidrIp("192.178.5.0/24"))
    );

    testRules(Collections.<NetworkAclEntry>emptyList(), permissions, true);
    assertResult(-1, 2);
  }

  @Test
  public void testOutboundAllowedRulesViolationInSecurityGroup() {
    List<IpPermission> permissions = Lists.<IpPermission>newArrayList(
        new IpPermission().withIpProtocol("tcp").withFromPort(6000).withToPort(7000).withIpv6Ranges(
            new Ipv6Range().withCidrIpv6("2001:0DB8:AC10:FE01::/32"))
    );

    testRules(Collections.<NetworkAclEntry>emptyList(), permissions, false);
    assertResult(-1, 2);
  }

  @Test
  public void testOutboundDeniedRulesViolationInSecurityGroup() {
    // When protocol is -1, the port range does not matter any more.
    List<IpPermission> permissions = Lists.<IpPermission>newArrayList(
        new IpPermission().withIpProtocol("-1").withFromPort(4000).withToPort(5000).withIpv6Ranges(
            new Ipv6Range().withCidrIpv6("2001:0DB8:AC10:FE01::/128"))
    );

    testRules(Collections.<NetworkAclEntry>emptyList(), permissions, false);
    assertResult(-1, 4);
  }

  @Test
  public void testSkippingNetworkACLValidation() {
    // Mock configuration
    Configured configuration = mock(SimpleConfiguration.class);
    when(configuration.getConfigurationValue(any(ConfigurationPropertyToken.class), any(LocalizationContext.class))).
        thenReturn("");

    // Mock security group
    SecurityGroup securityGroup = mock(SecurityGroup.class);
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Collections.singletonList(securityGroup));
    when(securityGroup.getIpPermissions()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);
    when(securityGroup.getIpPermissionsEgress()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);

    when(securityGroup.getGroupId()).thenReturn("sg-00000000");

    // Mock network ACL
    DescribeNetworkAclsResult aclResult = mock(DescribeNetworkAclsResult.class);
    when(ec2Client.describeNetworkAcls(any(DescribeNetworkAclsRequest.class)))
        .thenThrow(new AmazonServiceException("UnauthorizedOperation"));
    validator.validate("", configuration, accumulator, localizationContext);
    Map<String, Collection<PluginExceptionCondition>> conditionsByKey =
        accumulator.getConditionsByKey();
    Assert.assertEquals(conditionsByKey.keySet(), ImmutableSet.of(SECURITY_GROUP_KEY));
  }

  @Test
  public void testEmptyNetworkACL() {
    // Mock configuration
    Configured configuration = mock(SimpleConfiguration.class);
    when(configuration.getConfigurationValue(any(ConfigurationPropertyToken.class), any(LocalizationContext.class))).
        thenReturn("");

    // Mock security group
    SecurityGroup securityGroup = mock(SecurityGroup.class);
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Collections.singletonList(securityGroup));
    when(securityGroup.getIpPermissions()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);
    when(securityGroup.getIpPermissionsEgress()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);

    when(securityGroup.getGroupId()).thenReturn("sg-00000000");

    // Mock network ACL
    DescribeNetworkAclsResult aclResult = mock(DescribeNetworkAclsResult.class);
    when(ec2Client.describeNetworkAcls(any(DescribeNetworkAclsRequest.class)))
        .thenReturn(aclResult);
    when(aclResult.getNetworkAcls()).thenReturn(Collections.<NetworkAcl>emptyList());
    validator.validate("", configuration, accumulator, localizationContext);
    assertResult(1, -1);
  }

  @Test
  public void testMoreThanOneNetworkACLs() {
    // Mock configuration
    Configured configuration = mock(SimpleConfiguration.class);
    when(configuration.getConfigurationValue(any(ConfigurationPropertyToken.class), any(LocalizationContext.class))).
        thenReturn("");

    // Mock security group
    SecurityGroup securityGroup = mock(SecurityGroup.class);
    DescribeSecurityGroupsResult dsgResult = mock(DescribeSecurityGroupsResult.class);
    when(ec2Client.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
        .thenReturn(dsgResult);
    when(dsgResult.getSecurityGroups()).thenReturn(Collections.singletonList(securityGroup));
    when(securityGroup.getIpPermissions()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);
    when(securityGroup.getIpPermissionsEgress()).thenReturn(FULL_PERMISSION_SG_PERMISSIONS);
    when(securityGroup.getGroupId()).thenReturn("sg-00000000");

    // Mock network ACL
    NetworkAcl networkAcl1 = mock(NetworkAcl.class);
    NetworkAcl networkAcl2 = mock(NetworkAcl.class);
    when(networkAcl1.getNetworkAclId()).thenReturn("acl-00000001");
    when(networkAcl2.getNetworkAclId()).thenReturn("acl-00000002");
    DescribeNetworkAclsResult aclResult = mock(DescribeNetworkAclsResult.class);
    when(ec2Client.describeNetworkAcls(any(DescribeNetworkAclsRequest.class)))
        .thenReturn(aclResult);
    when(aclResult.getNetworkAcls()).thenReturn(ImmutableList.of(networkAcl1, networkAcl2));

    validator.validate("", configuration, accumulator, localizationContext);
    assertResult(1, -1);
  }

  @Test
  public void testInboundRulesInNetworkAcl() {
    List<NetworkAclEntry> entries = Lists.<NetworkAclEntry>newArrayList(
        new NetworkAclEntry().withRuleNumber(100).withProtocol("-1").withEgress(false)
            .withCidrBlock("54.23.205.0/12").withRuleAction("allow"),
        new NetworkAclEntry().withRuleNumber(200).withProtocol("-1").withEgress(false)
            .withCidrBlock("54.23.205.0/12").withRuleAction("deny")
    );

    testRules(entries, Collections.<IpPermission>emptyList(), false);
    assertResult(2, -1);
  }

  @Test
  public void testGetCidrFromEmptyAclEntry() {
    NetworkAclEntry networkAclEntry = new NetworkAclEntry();
    Assert.assertNull(EC2NetworkValidator.getCidrFromAclEntry(networkAclEntry));
    networkAclEntry = new NetworkAclEntry().withCidrBlock("").withIpv6CidrBlock("");
    Assert.assertNull(EC2NetworkValidator.getCidrFromAclEntry(networkAclEntry));
  }

  @Test
  public void testGetCidrFromIpV4AclEntry() {
    String ipv4Cidr = "102.233.12.0/24";
    NetworkAclEntry networkAclEntry = new NetworkAclEntry().withCidrBlock(ipv4Cidr);
    Assert.assertEquals(ipv4Cidr, EC2NetworkValidator.getCidrFromAclEntry(networkAclEntry));
    String ipv6Cidr = "2001:db8:ac10:fe01::/64";
    networkAclEntry = new NetworkAclEntry().withCidrBlock(ipv4Cidr).withIpv6CidrBlock(ipv6Cidr);
    Assert.assertEquals(ipv4Cidr, EC2NetworkValidator.getCidrFromAclEntry(networkAclEntry));
  }

  @Test
  public void testGetCidrFromIpV6AclEntry() {
    String ipv6Cidr = "2001:db8:ac10:fe01::/64";
    NetworkAclEntry networkAclEntry = new NetworkAclEntry().withIpv6CidrBlock(ipv6Cidr);
    Assert.assertEquals(ipv6Cidr, EC2NetworkValidator.getCidrFromAclEntry(networkAclEntry));
    networkAclEntry = new NetworkAclEntry().withCidrBlock("").withIpv6CidrBlock(ipv6Cidr);
    Assert.assertEquals(ipv6Cidr, EC2NetworkValidator.getCidrFromAclEntry(networkAclEntry));
  }

  @Test
  public void testGetPortRangeFromEmptyAclEntry() {
    NetworkAclEntry networkAclEntry = new NetworkAclEntry();
    Assert.assertNull(EC2NetworkValidator.getPortRangeFromAclEntry(networkAclEntry));
  }

  @Test
  public void testGetPortRangeFromAclEntryWithFromPort() {
    Range<Integer> expectedPorts = Range.singleton(80);
    PortRange portRange = new PortRange().withFrom(80);
    NetworkAclEntry networkAclEntry = new NetworkAclEntry().withPortRange(portRange);
    Assert.assertEquals(expectedPorts, EC2NetworkValidator.getPortRangeFromAclEntry(networkAclEntry));
  }

  @Test
  public void testGetPortRangeFromAclEntryWithToPort() {
    Range<Integer> expectedPorts = Range.singleton(80);
    PortRange portRange = new PortRange().withTo(80);
    NetworkAclEntry networkAclEntry = new NetworkAclEntry().withPortRange(portRange);
    Assert.assertEquals(expectedPorts, EC2NetworkValidator.getPortRangeFromAclEntry(networkAclEntry));
  }

  @Test
  public void testGetPortRangeFromAclEntryWithPorts() {
    Range<Integer> expectedPorts = Range.closed(8888, 9999);
    PortRange portRange = new PortRange().withFrom(8888).withTo(9999);
    NetworkAclEntry networkAclEntry = new NetworkAclEntry().withPortRange(portRange);
    Assert.assertEquals(expectedPorts, EC2NetworkValidator.getPortRangeFromAclEntry(networkAclEntry));
  }
}
