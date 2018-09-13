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
import com.google.common.collect.ArrayTable;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network rules.
 */
public final class NetworkRules {

  private static final Logger LOG = LoggerFactory.getLogger(NetworkRules.class);

  /**
   * The root key for network rules config.
   */
  private static final String ROOT_KEY = NetworkRules.class.getCanonicalName();

  /**
   * The table holding all network rules.
   */
  private final Table<Direction, AccessType, Iterable<NetworkRule>> rules;

  /**
   * A constant represents an empty set of network rules.
   */
  public static final NetworkRules EMPTY_RULES
      = new NetworkRules(ImmutableTable.of());

  /**
   * Internal constructor.
   *
   * @param rules a rules table
   */
  private NetworkRules(Table<Direction, AccessType, Iterable<NetworkRule>> rules) {
    Preconditions.checkNotNull(rules, "rules is null");
    this.rules = rules;
  }

  /**
   * Constructs a NetworkRules object using the given HOCON config.
   *
   * @param config a HOCON config
   * @return a NetworkRules object
   */
  public static NetworkRules fromConfig(Config config) {
    if (config == null) {
      return EMPTY_RULES;
    }

    try {
      return new NetworkRules(parse(config.getConfig(ROOT_KEY)));
    } catch (ConfigException | IllegalArgumentException e) {
      String msg = "Failed to parse network rules";
      LOG.error(msg, e);
      throw new IllegalArgumentException(msg, e);
    }
  }

  /**
   * Parses a HOCON config to a table of network rules.
   */
  private static Table<Direction, AccessType, Iterable<NetworkRule>> parse(Config config) {
    Table<Direction, AccessType, Iterable<NetworkRule>> rules =
        ArrayTable.create(EnumSet.allOf(Direction.class), EnumSet.allOf(AccessType.class));
    for (Direction direction : Direction.values()) {
      for (AccessType access : AccessType.values()) {
        populateRules(config, direction, access, rules);
      }
    }
    return rules;
  }

  /**
   * Populates the rule table.
   *
   * @param config    the HOCON config
   * @param direction the network direction
   * @param access    the network rule access type
   * @param rules     the rules table
   */
  private static void populateRules(Config config, Direction direction, AccessType access,
      Table<Direction, AccessType, Iterable<NetworkRule>> rules) {
    List<? extends Config> configList =
        config.getConfig(direction.toString().toLowerCase()).
            getConfigList(access.toString().toLowerCase());
    Set<NetworkRule> cell = Sets.newHashSet();
    for (Config val : configList) {
      Iterables.addAll(cell, NetworkRule.fromConfig(val, access));
    }
    rules.put(direction, access, cell);
  }

  /**
   * Returns the number of network rules.
   *
   * @return the number of network rules
   */
  public int size() {
    return this.rules.size();
  }

  /**
   * Checks if the network rule set is empty.
   *
   * @return true if it contains no network rules
   */
  public boolean isEmpty() {
    return this.rules.isEmpty();
  }

  /**
   * Retrieves rules based on the direction of network traffic.
   * <p>
   * <p>The returned rules are just copies of actual network rules. Changes to the
   * returned collection will not update the underlying NetworkRules, and vice versa.
   *
   * @param direction the network direction
   * @return the associated list of network rules
   */
  public Iterable<NetworkRule> getRules(Direction direction) {
    if (this.rules.isEmpty()) {
      return Collections.emptySet();
    }
    Set<NetworkRule> ruleSet = Sets.newHashSet();
    for (AccessType access : AccessType.values()) {
      Iterables.addAll(ruleSet, getRules(direction, access));
    }
    return ruleSet;
  }

  /**
   * Retrieves rules based on the network direction and access type.
   * <p>
   * <p>The returned rules are just copies of actual network rules. Changes to the
   * returned collection will not update the underlying NetworkRules, and vice versa.
   *
   * @param direction the network direction
   * @param access    the network rule access type
   * @return the associated list of network rules
   */
  public Iterable<NetworkRule> getRules(Direction direction, AccessType access) {
    return this.rules.isEmpty()
        ? Collections.emptySet()
        : Sets.newHashSet(this.rules.get(direction, access));
  }
}
