// (c) Copyright 2015 Cloudera, Inc.
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

package com.cloudera.director.aws;

import com.cloudera.director.aws.common.HoconConfigUtils;
import com.cloudera.director.spi.v2.util.Preconditions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import java.util.Collections;
import java.util.Map;

/**
 * Provides a nested hierarchy of named filter maps,
 * to be used for blacklists or whitelists.
 */
@SuppressWarnings("Guava")
public class AWSFilters {

  /**
   * A configuration key representing a blacklist.
   */
  public static final String BLACKLIST = "blacklist";

  /**
   * A configuration key representing a whitelist.
   */
  public static final String WHITELIST = "whitelist";

  /**
   * Empty filters.
   */
  public static final AWSFilters EMPTY_FILTERS =
      new AWSFilters(Optional.absent());

  /**
   * Returns AWS filters based on the specified configuration.
   *
   * @param config the underlying configuration
   * @return AWS filters based on the specified configuration
   */
  public static AWSFilters getAWSFilters(Config config) {
    return (config == null) ? EMPTY_FILTERS : new AWSFilters(Optional.of(config));
  }

  /**
   * The underlying configuration.
   */
  private Optional<Config> config;

  /**
   * Creates AWS filters with the specified parameters.
   *
   * @param config the underlying configuration
   */
  public AWSFilters(Optional<Config> config) {
    this.config = Preconditions.checkNotNull(config, "config is null");
  }

  /**
   * Returns the subfilters identified by the specified
   * sequence of keys.
   *
   * @param keys the keys
   * @return the subfilters identified by the specified
   * sequence of keys
   */
  public AWSFilters getSubfilters(String... keys) {
    if (config.isPresent()) {
      Config config = this.config.get();
      for (String key : keys) {
        try {
          config = config.getConfig(HoconConfigUtils.asConfigKey(key));
        } catch (ConfigException.Missing e) {
          return EMPTY_FILTERS;
        }
      }
      return getAWSFilters(config);
    } else {
      return EMPTY_FILTERS;
    }
  }

  /**
   * Returns the filter map for the specified key.
   *
   * @param key the key
   * @return the filter map for the specified key
   */
  public Map<String, String> getFilterMap(String key) {
    Map<String, String> filterMap;
    try {
      filterMap = config.isPresent()
          ? HoconConfigUtils.getStringMap(config.get(), HoconConfigUtils.asConfigKey(key))
          : Collections.emptyMap();
    } catch (Exception e) {
      filterMap = Collections.emptyMap();
    }
    return ImmutableMap.copyOf(filterMap);
  }

  /**
   * Returns whether the specified value is in the specified blacklist.
   *
   * @param key   the key identifying the specific blacklist to check
   * @param value the value to check
   * @return whether the specified value is in the specified blacklist
   */
  public boolean isBlacklisted(String key, String value) {
    return getBlacklistValue(key, value) != null;
  }

  /**
   * If the specified value is in the specified blacklist, returns the associated
   * blacklist value. Otherwise, returns {@code null}.
   *
   * @param key   the key identifying the specific blacklist to check
   * @param value the value to check
   * @return the blacklist value associated with the specified value in the specified blacklist,
   * or {@code null}
   */
  public String getBlacklistValue(String key, String value) {
    Map<String, String> blacklistMap = getSubfilters(key).getFilterMap(AWSFilters.BLACKLIST);
    return blacklistMap.get(value);
  }
}
