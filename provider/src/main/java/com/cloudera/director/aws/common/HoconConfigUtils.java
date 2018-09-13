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

package com.cloudera.director.aws.common;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utilities for dealing with typesafe config.
 */
public final class HoconConfigUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoconConfigUtils.class);

  /**
   * Returns a string that will be interpreted by the typesafe config library as a single
   * key instead of as a path through nested configurations.
   *
   * @param path the path, which may contain periods
   * @return the configuration key, will be interpreted by the typesafe config library as a single
   * unit instead of as a path through nested configurations
   */
  @SuppressWarnings("PMD.UselessParentheses")
  public static String asConfigKey(String path) {
    return ((path != null) && !path.startsWith("\"") && path.contains("."))
        ? "\"" + path + "\""
        : path;
  }

  /**
   * Returns a string that will be interpreted by the typesafe config library as a path through
   * nested configurations, instead of as a single key.
   *
   * @param key the key
   * @return the configuration path, will be interpreted by the typesafe config library as a path
   * through nested configurations, instead of as a single unit
   */
  public static String asConfigPath(String key) {
    return stripQuotes(key);
  }

  /**
   * Returns a key that has any matched pair of double quotes present at the beginning and end of
   * the key removed.
   *
   * @param key the key
   * @return the key without beginning and trailing quotes (if present)
   */
  @SuppressWarnings("PMD.UselessParentheses")
  public static String stripQuotes(String key) {
    return ((key != null) && key.startsWith("\"") && (key.length() > 1) && key.endsWith("\""))
        ? key.substring(1, key.length() - 1)
        : key;
  }

  /**
   * Returns a map view of the data associated with the specified key. If the key corresponds
   * to a string list, each element of the list will be mapped to itself. If the key corresponds
   * to a nested configuration, the map will contain those entries of the nested configuration which
   * have string values (but not, for example, further nested configurations). If the key
   * corresponds to some other type, throws an exception from the underlying typesafe config
   * implementation.
   *
   * @param config the configuration
   * @param key    the key
   * @return a map view of the data associated with the specified key
   */
  public static Map<String, String> getStringMap(Config config, String key) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    try {
      for (String value : config.getStringList(key)) {
        builder.put(value, value);
      }
    } catch (ConfigException.WrongType e) {
      Config nestedConfig = config.getConfig(key);
      for (Map.Entry<String, ConfigValue> entry : nestedConfig.root().entrySet()) {
        String nestedKey = entry.getKey();
        String quotelessKey = stripQuotes(nestedKey);
        try {
          builder.put(quotelessKey, nestedConfig.getString(nestedKey));
        } catch (ConfigException.WrongType ignore) {
          LOG.warn("Ignoring non-string-valued key: '{}'", quotelessKey);
        }
      }
    }
    return builder.build();
  }

  /**
   * Returns the specified optional configuration subsection.
   *
   * @param config     the optional configuration
   * @param sectionKey the optional desired configuration subsection
   * @return the specified optional configuration subsection
   */
  @SuppressWarnings("Guava")
  public static Config getSectionConfiguration(Config config, Optional<String> sectionKey) {
    return sectionKey.isPresent()
        ? getSectionConfiguration(config, sectionKey.get())
        : ConfigFactory.empty();
  }

  /**
   * Returns the specified configuration subsection.
   *
   * @param config     the configuration
   * @param sectionKey the desired configuration subsection
   * @return the specified configuration subsection
   */
  public static Config getSectionConfiguration(Config config,
      String sectionKey) {
    String quotedSectionKey = asConfigKey(sectionKey);
    return config.hasPath(quotedSectionKey)
        ? config.getConfig(quotedSectionKey)
        : ConfigFactory.empty();
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private HoconConfigUtils() {
  }
}
