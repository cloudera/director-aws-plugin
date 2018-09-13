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

package com.cloudera.director.aws.network;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.typesafe.config.Config;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The data model for a network rule.
 */
public final class NetworkRule {

  /**
   * The config key for protocol.
   */
  private final static String PROTOCOL_KEY = "protocol";

  /**
   * The config key for port.
   */
  private final static String PORT_KEY = "port";

  /**
   * The config key for ip range list.
   */
  private final static String IPRANGE_KEY = "ipRanges";

  /**
   * The network protocol that this rule applies to.
   */
  private final Protocol protocol;

  /**
   * The port that this rule applies to.
   * <p>
   * The valid port range is [0, 65535]. If it is -1, it means any port.
   */
  private final int port;

  /**
   * The IP ranges that this rule applies to.
   */
  private final CidrBlock cidrBlock;

  /**
   * The access type of this rule. It indicates if the network traffic defined
   * by this rule should be allowed or denied.
   */
  private final AccessType access;

  /**
   * Error message indicates a rule component is missing when parsing the HOCON config.
   */
  private final static String MISSING_ERROR_MSG = "Failed to create a network rule due to missing %s";

  /**
   * Error message indicates a rule component is invalid when parsing the HOCON config.
   */
  private final static String INVALID_ERROR_MSG = "Failed to create a network rule due to invalid %s";

  /**
   * Constructs an unmodifiable view of {@code Iterable} over the network rules described
   * by the given HOCON config.
   *
   * @param config the HOCON config
   * @param access the access type
   * @return an unmodifiable view of {@code Iterable} over the network rules described
   * by the given HOCON config.
   */
  public static Iterable<NetworkRule> fromConfig(Config config, AccessType access) {
    if (!config.hasPath(PROTOCOL_KEY)) {
      throw new IllegalArgumentException(String.format(MISSING_ERROR_MSG, PROTOCOL_KEY));
    }

    if (!config.hasPath(IPRANGE_KEY)) {
      throw new IllegalArgumentException(String.format(MISSING_ERROR_MSG, IPRANGE_KEY));
    }

    Protocol protocolFromConfig = Protocol.toProtocol(config.getString(PROTOCOL_KEY));
    if (protocolFromConfig == Protocol.UNKNOWN) {
      throw new IllegalArgumentException((String.format(INVALID_ERROR_MSG, PROTOCOL_KEY)));
    }

    List<String> ipRangesFromConfig = config.getStringList(IPRANGE_KEY);
    if (ipRangesFromConfig == null || ipRangesFromConfig.isEmpty()) {
      throw new IllegalArgumentException((String.format(INVALID_ERROR_MSG, IPRANGE_KEY)));
    }

    Set<NetworkRule> rules = new HashSet<>();
    // If the protocol is ALL, the port is set to -1 regardless of the value in the config.
    if (protocolFromConfig == Protocol.ALL) {
      for (String ipRange : ipRangesFromConfig) {
        rules.add(new NetworkRule(protocolFromConfig, -1, ipRange, access));
      }
    } else {
      if (!config.hasPath(PORT_KEY)) {
        throw new IllegalArgumentException(String.format(MISSING_ERROR_MSG, PORT_KEY));
      }
      int portFromConfig = config.getInt(PORT_KEY);
      if (!(-1 <= portFromConfig && portFromConfig <= 65535)) {
        throw new IllegalArgumentException((String.format(INVALID_ERROR_MSG, PORT_KEY)));
      }

      for (String ipRange : ipRangesFromConfig) {
        rules.add(new NetworkRule(protocolFromConfig, portFromConfig, ipRange, access));
      }
    }
    return Iterables.unmodifiableIterable(rules);
  }

  /**
   * Internal network rule constructor.
   */
  private NetworkRule(Protocol protocol, int port, String ipRange, AccessType access) {
    this.protocol = protocol;
    this.port = port;
    this.access = access;
    this.cidrBlock = CidrBlock.fromString(ipRange);
  }

  /**
   * Checks if this network rule is enforced by a network permission with the given parameters.
   *
   * @param protocolNameOrNumber the network protocol
   * @param portRange            the range of ports
   * @param ipRangeList          the IP range list
   * @param accessType           the access type
   * @return true if the network rule is enforced by the network permission
   */
  public boolean isEnforced(String protocolNameOrNumber,
      Range<Integer> portRange,
      List<String> ipRangeList,
      AccessType accessType) {
    if (this.access != accessType) {
      return false;
    }

    // Check the protocol
    Protocol proto = Protocol.toProtocol(protocolNameOrNumber);
    if (proto != Protocol.ALL && proto != this.protocol) {
      return false;
    }
    // Check the port
    if (proto != Protocol.ALL &&
        (portRange == null || (!portRange.contains(-1) && !portRange.contains(this.port)))) {
      return false;
    }
    // Check the ip ranges
    for (String curIpRange : ipRangeList) {
      CidrBlock curCidr = CidrBlock.fromString(curIpRange);
      if (curCidr.contains(this.cidrBlock)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if this network rule is violated by a network permission with the given parameters.
   *
   * @param protocolNameOrNumber the network protocol
   * @param portRange            the range of ports
   * @param ipRangeList          the IP range list
   * @param accessType           the access type
   * @return true if the network rule is violated by the network permission
   */
  public boolean isViolated(String protocolNameOrNumber,
      Range<Integer> portRange,
      List<String> ipRangeList,
      AccessType accessType) {
    if (this.access == accessType) {
      return false;
    }

    // Check the protocol
    Protocol proto = Protocol.toProtocol(protocolNameOrNumber);
    if (this.protocol != Protocol.ALL && proto != Protocol.ALL && proto != this.protocol) {
      return false;
    }
    // Check the port
    if (this.protocol != Protocol.ALL && proto != Protocol.ALL &&
        (this.port != -1 && !portRange.contains(-1) && !portRange.contains(this.port))) {
      return false;
    }
    // Check the ip ranges
    for (String curIpRange : ipRangeList) {
      CidrBlock curCidr = CidrBlock.fromString(curIpRange);
      if (curCidr.contains(this.cidrBlock) || this.cidrBlock.contains(curCidr)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("protocol", this.protocol)
        .add("port", this.port)
        .add("cidrBlock", this.cidrBlock)
        .add("access", this.access)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NetworkRule that = (NetworkRule) o;
    return port == that.port &&
        protocol == that.protocol &&
        Objects.equals(cidrBlock, that.cidrBlock) &&
        access == that.access;
  }

  @Override
  public int hashCode() {
    return Objects.hash(protocol, port, cidrBlock, access);
  }
}
