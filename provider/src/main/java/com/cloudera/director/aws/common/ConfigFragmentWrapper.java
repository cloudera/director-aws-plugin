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

import static com.google.common.base.Preconditions.checkNotNull;

import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.util.ConfigurationPropertiesUtil;
import com.typesafe.config.Config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * Wraps a HOCON config fragment object as an SPI configured object.
 *
 * @see com.typesafe.config.Config
 * @see com.cloudera.director.spi.v2.model.Configured
 */
public class ConfigFragmentWrapper implements Configured {

  private final Config config;
  private final List<ConfigurationProperty> properties;

  /**
   * Constructs a new wrapper around a config instance.
   *
   * @param config     fragment of a config object
   * @param components properties that should be exposed
   *                   from the underlying object
   */
  public ConfigFragmentWrapper(Config config, List<ConfigurationProperty>... components) {
    this.config = checkNotNull(config, "config is null");
    this.properties = ConfigurationPropertiesUtil.merge(components);
  }

  /**
   * Builds a map with all the configuration properties. Mandatory missing
   * properties are just ignored and the result is immutable.
   *
   * @param localizationContext the localization context
   */
  @Override
  @SuppressWarnings("PMD.EmptyCatchBlock")
  public Map<String, String> getConfiguration(LocalizationContext localizationContext) {
    Map<String, String> result = new HashMap<>();
    for (ConfigurationProperty property : properties) {
      try {
        result.put(property.getConfigKey(), getConfigurationValue(property, localizationContext));

      } catch (NoSuchElementException ignore) {
        // a missing required property is not an error
        // when trying to retrieve them as map
      }
    }
    return Collections.unmodifiableMap(result);
  }

  /**
   * Gets the value of a configuration property from the underlying config object.
   *
   * @param token               a description for a configuration property
   * @param localizationContext the localization context
   * @throws NoSuchElementException on missing required properties
   */
  @Override
  public String getConfigurationValue(ConfigurationPropertyToken token,
      LocalizationContext localizationContext) {
    return getConfigurationValue(token.unwrap(), localizationContext);
  }

  /**
   * Gets the value of a configuration property from the underlying config object.
   *
   * @param property            a description for a configuration property
   * @param localizationContext the localization context
   * @throws NoSuchElementException on missing required properties
   */
  @Override
  public String getConfigurationValue(ConfigurationProperty property,
      LocalizationContext localizationContext) {
    String configKey = property.getConfigKey();

    if (config.hasPath(configKey)) {
      return config.getString(configKey);

    } else if (property.isRequired()) {
      throw new NoSuchElementException(property.getMissingValueErrorMessage(localizationContext));
    }

    return property.getDefaultValue();
  }

  /**
   * Dumps all the properties as multiple log lines for debugging.
   *
   * @param description         the meaning of this set of properties
   * @param logger              a logger used as output
   * @param localizationContext the localization context
   */
  public void dump(String description, Logger logger, LocalizationContext localizationContext) {
    logger.info(description);

    for (ConfigurationProperty property : properties) {
      String value;
      try {
        value = getConfigurationValue(property, localizationContext);

      } catch (NoSuchElementException e) {
        value = "(not set)";
      }

      logger.info(String.format("* %s (%s, %s) = %s",
          property.getConfigKey(),
          property.isRequired() ? "required" : "optional",
          property.isSensitive() ? "sensitive" : "regular",
          property.isSensitive() ? "REDACTED" : value
      ));
    }
  }
}
