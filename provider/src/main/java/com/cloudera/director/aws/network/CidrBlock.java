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

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * The CIDR (Classless Inter-Domain Routing) block represents a range af
 * IP addresses. It can handle both IPv4 and IPv6 addresses.
 */
public final class CidrBlock {

  /**
   * The network address of the CIDR block.
   */
  private final InetAddress baseAddress;

  /**
   * The number of bits of the prefix in CIDR notation.
   */
  private final int prefixLen;

  /**
   * The separator used to separate the IP address part and prefix part in CIDR notation.
   */
  private static final char SEPARATOR = '/';

  /**
   * Constructs a CidrBlock from CIDR notation.
   *
   * @param notation CIDR notation representing a block of IP addresses
   * @return         a new CidrBlock object
   */
  public static CidrBlock fromString(String notation) {
    Preconditions.checkNotNull(notation);
    final String error = "Invalid CIDR notation: " + notation;

    int idx = notation.indexOf(SEPARATOR);
    if (idx < 0) {
      throw new IllegalArgumentException(error);
    }

    InetAddress addr;
    String addrPart = notation.substring(0, idx);
    try {
      addr = InetAddresses.forString(addrPart);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(error);
    }

    int prefixLength;
    String prefixLenPart = notation.substring(idx + 1);
    try {
      prefixLength = Integer.parseInt(prefixLenPart);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(error);
    }
    return new CidrBlock(addr, prefixLength);
  }

  /**
   * Internal constructor.
   *
   * @param ipAddress an IP address (either IPv4 or Ipv6)
   * @param prefixLen the number of bits of the prefix in CIDR notation
   */
  private CidrBlock(InetAddress ipAddress, int prefixLen) {
    this.baseAddress = mask(ipAddress, prefixLen);
    this.prefixLen = prefixLen;
  }

  /**
   * Uses the routing prefix information to mask an IP address to a network address.
   *
   * @param ipAddress an IP address (either IPv4 or Ipv6)
   * @param prefixLen the number of bits of the prefix in CIDR notation
   * @return          the network address of the CIDR block where this IP address falls
   */
  private static InetAddress mask(InetAddress ipAddress, int prefixLen) {
    byte[] bytes = ipAddress.getAddress();
    Preconditions.checkArgument(prefixLen >= 0 && prefixLen <= bytes.length * 8,
        String.format("The CIDR prefix length %d is out of range: [0, %d].",
            prefixLen, (bytes.length * 8)));

    int borderByte = (prefixLen == 0) ? 0 : (prefixLen - 1) / 8;
    int numBits = prefixLen - (borderByte * 8);
    int bitMask = (-1 << (8 - numBits));

    bytes[borderByte] = (byte) (bytes[borderByte] & bitMask);
    for (int i = borderByte + 1; i < bytes.length; i++) {
      bytes[i] = 0;
    }

    try {
      return InetAddress.getByAddress(bytes);
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Failed to apply network mask");
    }
  }

  /**
   * Checks if the given IP address is included in this CIDR block.
   *
   * @param address a IP address (either IPv4 or IPv6)
   * @return        true if this IP address is included in this CIDR block
   */
  public boolean contains(InetAddress address) {
    Preconditions.checkNotNull(address);

    return (baseAddress.getClass() == address.getClass())
        && baseAddress.equals(mask(address, prefixLen));
  }

  /**
   * Checks if the given CIDR block is a subset of the current CIDR block.
   *
   * @param cidr the CIDR block representing the other block of IP addresses
   * @return     true if the given CIDR block is a subset of the current one
   */
  public boolean contains(CidrBlock cidr) {
    Preconditions.checkNotNull(cidr);

    return this.prefixLen <= cidr.prefixLen && this.contains(cidr.baseAddress);
  }

  /**
   * The network address of the current CIDR block.
   *
   * @return the network address of the CIDR block
   */
  public InetAddress getBaseAddress() {
    return baseAddress;
  }

  /**
   * The number of bits of the prefix in the CIDR block.
   *
   * @return the number of bits of the prefix in the CIDR block
   */
  public int getPrefixLength() {
    return prefixLen;
  }

  @Override
  public String toString() {
    return String.format("%s%c%d", InetAddresses.toAddrString(baseAddress), SEPARATOR, prefixLen);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CidrBlock that = (CidrBlock) o;
    return prefixLen == that.prefixLen
        && baseAddress.equals(that.baseAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseAddress, prefixLen);
  }
}
