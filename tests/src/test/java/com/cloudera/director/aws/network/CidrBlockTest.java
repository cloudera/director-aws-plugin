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

import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for CidrBlock.
 */
public class CidrBlockTest {

  @Test
  public void testIPv4Address() {
    String ipv4Cidr = "192.168.50.23/24";
    CidrBlock notation = CidrBlock.fromString(ipv4Cidr);
    assertEquals(InetAddresses.forString("192.168.50.0"),
        notation.getBaseAddress());
    assertEquals(24, notation.getPrefixLength());
  }

  @Test
  public void testIPv4WithZeroPrefix() {
    String ipv4Cidr = "1.2.3.4/0";
    CidrBlock notation = CidrBlock.fromString(ipv4Cidr);
    assertEquals(InetAddresses.forString("0.0.0.0"),
        notation.getBaseAddress());
    assertEquals(0, notation.getPrefixLength());
  }

  @Test
  public void testIPv4With32Prefix() {
    String ipv4Cidr = "1.2.3.4/32";
    CidrBlock notation = CidrBlock.fromString(ipv4Cidr);
    assertEquals(InetAddresses.forString("1.2.3.4"),
        notation.getBaseAddress());
    assertEquals(32, notation.getPrefixLength());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testIPv4WithNegativePrefix() {
    String ipv4Cidr = "1.2.3.4/-1";
    CidrBlock.fromString(ipv4Cidr);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testIPv4WithLargerPrefix() {
    String ipv4Cidr = "1.2.3.4/33";
    CidrBlock.fromString(ipv4Cidr);
  }

  @Test
  public void testIPv6Address() {
    String ipv6Cidr = "1:2:3:4:5:6:7:8/64";
    CidrBlock notation = CidrBlock.fromString(ipv6Cidr);
    assertEquals(InetAddresses.forString("1:2:3:4:0:0:0:0"),
        notation.getBaseAddress());
    assertEquals(64, notation.getPrefixLength());
  }

  @Test
  public void testIPv6AddressWithZeroPrefix() {
    String ipv4Cidr = "2001:db8:7:6:5:4:3:2/0";
    CidrBlock notation = CidrBlock.fromString(ipv4Cidr);
    assertEquals(InetAddresses.forString("0:0:0:0:0:0:0:0"),
        notation.getBaseAddress());
    assertEquals(0, notation.getPrefixLength());
  }

  @Test
  public void testIPv6AddressWith128Prefix() {
    String ipv4Cidr = "2001:db8:7:6:5:4:3:2/128";
    CidrBlock notation = CidrBlock.fromString(ipv4Cidr);
    assertEquals(InetAddresses.forString("2001:db8:7:6:5:4:3:2"),
        notation.getBaseAddress());
    assertEquals(128, notation.getPrefixLength());
  }

  @Test
  public void testIPv6AddressAbbr() {
    String ipv6Cidr = "2001:db8:ac10:fe01::/64";
    CidrBlock notation = CidrBlock.fromString(ipv6Cidr);
    assertEquals(InetAddresses.forString("2001:db8:ac10:fe01:0:0:0:0"),
        notation.getBaseAddress());
    assertEquals(64, notation.getPrefixLength());
  }

  @Test
  public void testIPv6AddressWithLeadingZero() {
    String ipv6Cidr = "2001:00b8:0c10:fe01::/64";
    CidrBlock notation = CidrBlock.fromString(ipv6Cidr);
    assertEquals(InetAddresses.forString("2001:b8:c10:fe01:0:0:0:0"),
        notation.getBaseAddress());
    assertEquals(64, notation.getPrefixLength());
  }

  @Test
  public void testIPv6AddressInUppercase() {
    String ipv6Cidr = "2001:DB8:AC10:FE01::/64";
    CidrBlock notation = CidrBlock.fromString(ipv6Cidr);
    assertEquals(InetAddresses.forString("2001:db8:ac10:fe01:0:0:0:0"),
        notation.getBaseAddress());
    assertEquals(64, notation.getPrefixLength());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testInvalidIPv6Address() {
    String ipv6Cidr = "2001:db8:ac10:fe01::::/64";
    CidrBlock.fromString(ipv6Cidr);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testIPv6AddressWithNegativePrefix() {
    String ipv6Cidr = "2001:DB8:AC10:FE01::/-1";
    CidrBlock.fromString(ipv6Cidr);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testIPv6AddressWithLargerPrefix() {
    String ipv6Cidr = "2001:DB8:AC10:FE01::/129";
    CidrBlock.fromString(ipv6Cidr);
  }

  @Test
  public void testContainsIpV4Addr() {
    CidrBlock block = CidrBlock.fromString("201.98.0.0/16");
    InetAddress addr = InetAddresses.forString("201.98.155.25");
    assertTrue(block.contains(addr));
  }

  @Test
  public void testNotContainsIpV4Addr() {
    CidrBlock block = CidrBlock.fromString("201.98.0.0/16");
    InetAddress addr = InetAddresses.forString("201.99.155.25");
    assertFalse(block.contains(addr));
  }

  @Test
  public void testContainsIpV4Self() {
    String cidr = "201.98.0.0/16";
    CidrBlock block1 = CidrBlock.fromString(cidr);
    CidrBlock block2 = CidrBlock.fromString(cidr);
    assertTrue(block1.contains(block2));
  }

  @Test
  public void testContainsIpV4CidrBlock() {
    CidrBlock block = CidrBlock.fromString("201.98.0.0/16");
    CidrBlock subBlock = CidrBlock.fromString("201.98.123.0/24");
    assertTrue(block.contains(subBlock));
    assertFalse(subBlock.contains(block));
  }

  @Test
  public void testContainsIpV6Addr() {
    CidrBlock block = CidrBlock.fromString("2001:DB8:AC10:FE01::/64");
    InetAddress addr = InetAddresses.forString("2001:DB8:AC10:FE01:5:4::");
    assertTrue(block.contains(addr));
  }

  @Test
  public void testNotContainsIpV6Addr() {
    CidrBlock block = CidrBlock.fromString("2001:DB8:AC10:FE01::/64");
    InetAddress addr = InetAddresses.forString("2001:DB8:AC10:FE07:5:4::");
    assertFalse(block.contains(addr));
  }

  @Test
  public void testContainsIpV6Self() {
    String cidr = "2001:DB8:AC10:FE01::/64";
    CidrBlock block1 = CidrBlock.fromString(cidr);
    CidrBlock block2 = CidrBlock.fromString(cidr);
    assertTrue(block1.contains(block2));
  }

  @Test
  public void testContainsIpV6CidrBlock() {
    CidrBlock block = CidrBlock.fromString("2001:DB8:AC10:FE01::/64");
    CidrBlock subBlock = CidrBlock.fromString("2001:DB8:AC10:FE01:2AC::/80");
    assertTrue(block.contains(subBlock));
    assertFalse(subBlock.contains(block));
  }

  @Test
  public void testEqualIpV4() {
    CidrBlock block1 = CidrBlock.fromString("201.98.0.0/16");
    CidrBlock block2 = CidrBlock.fromString("201.98.0.0/16");
    assertTrue(block1.equals(block2) && block2.equals(block1));
    assertTrue(block1.hashCode() == block2.hashCode());

    Set<CidrBlock> blockSet = Sets.newHashSet(block1, block2);
    assertTrue(blockSet.size() == 1);
  }

  @Test
  public void testEqualIpV6() {
    CidrBlock block1 = CidrBlock.fromString("2001:DB8:AC10:FE01::/64");
    CidrBlock block2 = CidrBlock.fromString("2001:DB8:AC10:FE01::/64");
    assertTrue(block1.equals(block2) && block2.equals(block1));
    assertTrue(block1.hashCode() == block2.hashCode());

    Set<CidrBlock> blockSet = Sets.newHashSet(block1, block2);
    assertTrue(blockSet.size() == 1);
  }
}
