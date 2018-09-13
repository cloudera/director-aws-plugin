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

/**
 * The network protocols that are supported by {@code NetworkRule}.
 * <p>
 * <p>
 * Each element contains an IP protocol name (<code>tcp</code>, <code>udp</code>)
 * and the associated protocol number (see <a
 * href="http://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml">
 * Protocol Numbers</a>).
 * </p>
 */
public enum Protocol {
  /**
   * Special value, meaning any protocol.
   */
  ALL(-1),
  /**
   * TCP protocol.
   */
  TCP(6),
  /**
   * UDP protocol.
   */
  UDP(17),
  /**
   * Unknown protocol.
   */
  UNKNOWN(Integer.MIN_VALUE);

  private int number;

  Protocol(int number) {
    this.number = number;
  }

  /**
   * Converts to a protocol enum element based on either protocol name or protocol number.
   *
   * @param nameOrNumber a protocol name or protocol number
   * @return a protocol enum element
   */
  public static Protocol toProtocol(String nameOrNumber) {
    Preconditions.checkNotNull(nameOrNumber);
    try {
      Integer curNumber = Integer.parseInt(nameOrNumber);
      for (Protocol val : values()) {
        if (val.number == curNumber) {
          return val;
        }
      }
      return UNKNOWN;
    } catch (NumberFormatException nfe) {
      try {
        return valueOf(nameOrNumber.toUpperCase());
      } catch (IllegalArgumentException iae) {
        return UNKNOWN;
      }
    }
  }
}
