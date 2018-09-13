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

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.provider.EC2InstanceTemplateConfigurationValidator.INVALID_SECURITY_GROUP;
import static com.cloudera.director.spi.v2.model.util.Validations.addError;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeNetworkAclsRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkAclsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.Ipv6Range;
import com.amazonaws.services.ec2.model.NetworkAcl;
import com.amazonaws.services.ec2.model.NetworkAclEntry;
import com.amazonaws.services.ec2.model.PortRange;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.network.AccessType;
import com.cloudera.director.aws.network.Direction;
import com.cloudera.director.aws.network.NetworkRule;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates AWS network settings against pre-defined network rules.
 */
public final class EC2NetworkValidator implements ConfigurationValidator {

  private static final Logger LOG = LoggerFactory.getLogger(EC2NetworkValidator.class);

  /**
   * The EC2 provider.
   */
  private final EC2Provider provider;

  /**
   * The pre-defined network rules.
   */
  private final NetworkRules networkRules;

  private static final String INVALID_ENFORCEMENT_SECURITY_GROUP =
      "No security groups enforce %s network rule: %s";

  private static final String INVALID_VIOLATION_SECURITY_GROUP =
      "Security group with identifier %s violates %s network rule: %s";

  private static final String EMPTY_NETWORK_ACL =
      "Cannot find the associated network ACL for subnet: %s";

  private static final String MORE_THAN_ONE_NETWORK_ACL =
      "Found more than one associated network ACL %s for subnet: %s";

  private static final String INVALID_ENFORCEMENT_NETWORK_ACL =
      "No network ACLs enforce %s network rule: %s";

  private static final String INVALID_VIOLATION_NETWORK_ACL =
      "Network ACL with identifier %s violates %s network rule: %s";

  /**
   * Creates an EC2 network validator with the given EC2 provider.
   *
   * @param provider the EC2 provider
   */
  public EC2NetworkValidator(EC2Provider provider) {
    this.provider = Preconditions.checkNotNull(provider, "provider");
    this.networkRules = provider.getNetworkRules();
  }

  /**
   * Validates the AWS security group settings based on the pre-defined
   * network rules configurations.
   *
   * @param name                the name of the object being validated
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @Override
  public void validate(String name,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    if (this.networkRules.isEmpty()) {
      return;
    }

    AmazonEC2Client ec2Client = provider.getClient();
    checkSecurityGroups(ec2Client, configuration, accumulator, localizationContext);
    checkNetworkACL(ec2Client, configuration, accumulator, localizationContext);
  }

  /**
   * Network ACL entry comparator.
   */
  private static class NetworkAclEntryComparator implements Comparator<NetworkAclEntry>, Serializable {

    @Override
    public int compare(NetworkAclEntry entry1, NetworkAclEntry entry2) {
      return entry1.getRuleNumber().compareTo(entry2.getRuleNumber());
    }
  }

  /**
   * Validates the network ACL against the pre-defined network rules.
   * <p>
   * <p>
   * For more information about network ACLs, see <a
   * href="http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_ACLs.html">Network ACLs</a> in the <i>Amazon
   * Virtual Private Cloud User Guide</i>.
   * </p>
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @SuppressWarnings("Guava")
  private void checkNetworkACL(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    String subnetId = configuration.getConfigurationValue(SUBNET_ID, localizationContext);
    DescribeNetworkAclsResult aclResult;
    LOG.info(">> Describing network ACL associated with subnet '{}'", subnetId);
    try {
      aclResult = client.describeNetworkAcls(
          new DescribeNetworkAclsRequest().withFilters(
              new Filter().withName("association.subnet-id").withValues(subnetId)
          )
      );
    } catch (AmazonServiceException e) {
      // Due to backward compatibility, we cannot mandate the IAM permission:
      // ec2:DescribeNetworkAcls in customers' accounts and have to fail the
      // above AWS call gracefully, which means the NetworkACL validation is
      // optional now.
      // We have logged a ticket, https://jira.cloudera.com/browse/CLOUD-5345,
      // to track it, and will make this validation mandatory later.
      LOG.warn("Failed to retrieve the network ACL for subnet: " + subnetId, e);
      LOG.warn("Skipping network ACL validation");
      return;
    }
    List<NetworkAcl> aclList = aclResult.getNetworkAcls();
    // Each subnet must be associated with one and only one network ACL.
    if (aclList.isEmpty()) {
      LOG.error(String.format(EMPTY_NETWORK_ACL, subnetId));
      addError(accumulator, SUBNET_ID, localizationContext, null,
          EMPTY_NETWORK_ACL, subnetId);
      return;
    }
    if (aclList.size() > 1) {
      List<String> aclIds = FluentIterable.from(aclList)
          .transform(NetworkAcl::getNetworkAclId)
          .toList();
      LOG.error(String.format(MORE_THAN_ONE_NETWORK_ACL, aclIds, subnetId));
      addError(accumulator, SUBNET_ID, localizationContext, null,
          MORE_THAN_ONE_NETWORK_ACL, aclIds, subnetId);
      return;
    }

    NetworkAcl networkAcl = aclList.get(0);

    for (final Direction direction : Direction.values()) {
      Iterable<NetworkAclEntry> aclEntries = FluentIterable.from(networkAcl.getEntries())
          .filter(aclEntry -> direction == Direction.INBOUND
              ? !aclEntry.isEgress()
              : aclEntry.isEgress())
          .toSortedList(new NetworkAclEntryComparator());

      checkRulesForNetworkAclEntries(networkAcl.getNetworkAclId(), aclEntries, direction,
          accumulator, localizationContext);
    }
  }

  /**
   * Checks network ACL entries against pre-defined network rules.
   * <p>
   * Because network ACLs define both allow rules and deny rules, we need check
   * the enforcements and violations for both allow and deny rules.
   *
   * @param networkAclId        the network ACL ID
   * @param sortedEntries       the sorted list of network ACL entries
   * @param direction           the network traffic direction
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  private void checkRulesForNetworkAclEntries(String networkAclId,
      Iterable<NetworkAclEntry> sortedEntries,
      Direction direction,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    Iterable<NetworkRule> rules = networkRules.getRules(direction);
    if (Iterables.isEmpty(rules)) {
      return;
    }

    List<NetworkRule> pendingRules = Lists.newArrayList(rules);
    Multimap<String, NetworkRule> violatedRules = HashMultimap.create();

    for (NetworkAclEntry aclEntry : sortedEntries) {
      String cidr = getCidrFromAclEntry(aclEntry);
      if (cidr != null) {
        final List<String> ipRanges = ImmutableList.of(cidr);
        final String protocol = aclEntry.getProtocol();
        final Range<Integer> ports = getPortRangeFromAclEntry(aclEntry);
        final AccessType accessType =
            AccessType.valueOf(aclEntry.getRuleAction().toUpperCase(localizationContext.getLocale()));

        Iterator<NetworkRule> ruleIt = pendingRules.iterator();
        while (ruleIt.hasNext()) {
          NetworkRule rule = ruleIt.next();
          if (rule.isEnforced(protocol, ports, ipRanges, accessType)) {
            ruleIt.remove();
          } else if (rule.isViolated(protocol, ports, ipRanges, accessType)) {
            violatedRules.put(networkAclId, rule);
            ruleIt.remove();
          }
        }
      }
    }
    recordNotEnforcedRules(pendingRules, direction, accumulator, localizationContext,
        INVALID_ENFORCEMENT_NETWORK_ACL, SUBNET_ID);
    recordViolatedRules(violatedRules, direction, accumulator, localizationContext,
        INVALID_VIOLATION_NETWORK_ACL, SUBNET_ID);
  }

  /**
   * Gets the port range for the TCP and UDP protocols defined in the given network
   * ACL entry. If the range contains {@code -1}, it indicates all ports.
   *
   * @param aclEntry the given network ACL entry
   * @return the port range that this network ACL entry applies to
   */
  @VisibleForTesting
  static Range<Integer> getPortRangeFromAclEntry(NetworkAclEntry aclEntry) {
    PortRange portRange = aclEntry.getPortRange();
    Range<Integer> ports = null;
    if (portRange != null) {
      if (portRange.getFrom() != null && portRange.getTo() != null) {
        ports = Range.closed(portRange.getFrom(), portRange.getTo());
      } else {
        if (portRange.getFrom() != null) {
          ports = Range.singleton(portRange.getFrom());
        } else if (portRange.getTo() != null) {
          ports = Range.singleton(portRange.getTo());
        }
      }
    }
    return ports;
  }

  /**
   * Gets the IP ranges defined in the given network ACL entry.
   *
   * @param aclEntry the given network ACL entry
   * @return the IP range that the network ACL entry applies to
   */
  @VisibleForTesting
  static String getCidrFromAclEntry(NetworkAclEntry aclEntry) {
    String ipv4Cidr = aclEntry.getCidrBlock();
    if (!Strings.isNullOrEmpty(ipv4Cidr)) {
      return ipv4Cidr;
    } else {
      String ipv6Cidr = aclEntry.getIpv6CidrBlock();
      if (!Strings.isNullOrEmpty(ipv6Cidr)) {
        return ipv6Cidr;
      }
    }
    return null;
  }

  /**
   * Validates the actual security group permissions against the pre-defined network rules.
   *
   * @param client              the EC2 client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  private void checkSecurityGroups(AmazonEC2Client client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    List<String> securityGroupIds = EC2InstanceTemplate.CSV_SPLITTER.splitToList(
        configuration.getConfigurationValue(SECURITY_GROUP_IDS, localizationContext));
    List<SecurityGroup> securityGroups = Collections.emptyList();
    try {
      DescribeSecurityGroupsResult result = client.describeSecurityGroups(
          new DescribeSecurityGroupsRequest().withGroupIds(securityGroupIds));
      securityGroups = result.getSecurityGroups();
    } catch (AmazonServiceException e) {
      if (!e.getErrorCode().startsWith(INVALID_SECURITY_GROUP)) {
        throw Throwables.propagate(e);
      }
      // Otherwise, the error should be caught in anther validation,
      // EC2InstanceTemplateConfigurationValidator.checkSecurityGroupIds()
    }

    for (Direction direction : Direction.values()) {
      checkRulesForSecurityGroups(securityGroups, direction, accumulator,
          localizationContext);
    }
  }

  /**
   * Checks the inbound/outbound security group permissions against pre-defined network
   * rules.
   * <p>
   * Because security groups only define allow rules, we only need check the enforcement for
   * allow rules, and check violations for deny rules.
   *
   * @param securityGroups      a list of security groups
   * @param direction           the network traffic direction
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
  private void checkRulesForSecurityGroups(List<SecurityGroup> securityGroups,
      Direction direction,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    Iterable<NetworkRule> allowRules = networkRules.getRules(direction, AccessType.ALLOW);
    Iterable<NetworkRule> denyRules = networkRules.getRules(direction, AccessType.DENY);

    if (Iterables.isEmpty(allowRules) && Iterables.isEmpty(denyRules)) {
      return;
    }

    List<NetworkRule> notEnforcedRules = Lists.newArrayList(allowRules);
    Multimap<String, NetworkRule> violatedRules = HashMultimap.create();

    for (SecurityGroup sg : securityGroups) {
      List<IpPermission> permissions =
          direction == Direction.INBOUND ? sg.getIpPermissions() : sg.getIpPermissionsEgress();
      for (IpPermission permission : permissions) {
        final String protocol = permission.getIpProtocol();
        if ("icmp".equalsIgnoreCase(protocol)) {
          LOG.debug("Skipping check for ICMP permission {}", permission.toString());
          continue;
        }
        final Range<Integer> ports = getPortRangeFromSgPermission(permission);
        final List<String> ipRanges = getIpRangesFromSgPermission(permission);
        if (!ipRanges.isEmpty()) {
          // Check enforcements for allow rules
          Iterables.removeIf(notEnforcedRules,
              rule -> rule.isEnforced(protocol, ports, ipRanges, AccessType.ALLOW));

          // Check violations for deny rules
          violatedRules.putAll(sg.getGroupId(), Iterables.filter(denyRules,
              rule -> rule.isViolated(protocol, ports, ipRanges, AccessType.ALLOW)));
        }
      }
    }

    recordNotEnforcedRules(notEnforcedRules, direction, accumulator, localizationContext,
        INVALID_ENFORCEMENT_SECURITY_GROUP, SECURITY_GROUP_IDS);
    recordViolatedRules(violatedRules, direction, accumulator, localizationContext,
        INVALID_VIOLATION_SECURITY_GROUP, SECURITY_GROUP_IDS);
  }

  /**
   * Errors out not-enforced network rules.
   *
   * @param notEnforcedRules    the not-enforced network rules
   * @param direction           the network traffic direction
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   * @param msgFormat           the error message format string
   * @param token               the token representing the configuration property in error
   */
  private void recordNotEnforcedRules(Iterable<NetworkRule> notEnforcedRules,
      Direction direction,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext,
      String msgFormat,
      ConfigurationPropertyToken token) {
    for (NetworkRule rule : notEnforcedRules) {
      LOG.error(String.format(msgFormat,
          direction.toString().toLowerCase(localizationContext.getLocale()), rule));
      addError(accumulator, token, localizationContext, null,
          msgFormat, direction.toString().toLowerCase(localizationContext.getLocale()), rule);
    }
  }

  /**
   * Errors out violated network rules.
   *
   * @param violatedRules       the violated network rules
   * @param direction           the network traffic direction
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   * @param msgFormat           the error message format string
   * @param token               the token representing the configuration property in error
   */
  private void recordViolatedRules(Multimap<String, NetworkRule> violatedRules,
      Direction direction,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext,
      String msgFormat,
      ConfigurationPropertyToken token) {
    for (String sg : violatedRules.keySet()) {
      Collection<NetworkRule> rules = violatedRules.get(sg);
      LOG.error(String.format(msgFormat,
          sg, direction.toString().toLowerCase(localizationContext.getLocale()), rules));
      addError(accumulator, token, localizationContext, null,
          msgFormat, sg, direction.toString().toLowerCase(localizationContext.getLocale()), rules);
    }
  }

  /**
   * Gets the port range for the TCP and UDP protocols defined in the given security
   * group rule.
   *
   * @param permission the given security group rule
   * @return the port range that this security group rule applies to
   */
  private Range<Integer> getPortRangeFromSgPermission(IpPermission permission) {
    Integer fromPort = permission.getFromPort();
    Integer toPort = permission.getToPort();
    Range<Integer> ports = null;
    if (fromPort != null && toPort != null) {
      ports = Range.closed(fromPort, toPort);
    } else {
      if (fromPort != null) {
        ports = Range.singleton(fromPort);
      } else if (toPort != null) {
        ports = Range.singleton(toPort);
      }
    }
    return ports;
  }

  /**
   * Gets the IP ranges defined in the given security group rule.
   *
   * @param permission the given security group rule
   * @return a list of IP ranges that the security group rule applies to
   */
  private List<String> getIpRangesFromSgPermission(IpPermission permission) {
    List<String> cidrs = Lists.newArrayList();
    List<IpRange> ipRanges = permission.getIpv4Ranges();
    if (ipRanges != null && !ipRanges.isEmpty()) {
      for (IpRange ipRange : ipRanges) {
        cidrs.add(ipRange.getCidrIp());
      }
    } else {
      List<Ipv6Range> ipv6Ranges = permission.getIpv6Ranges();
      if (ipv6Ranges != null && !ipv6Ranges.isEmpty()) {
        for (Ipv6Range ipv6Range : ipv6Ranges) {
          cidrs.add(ipv6Range.getCidrIpv6());
        }
      }
    }
    return cidrs;
  }
}
