// (c) Copyright 2016 Cloudera, Inc.

package com.cloudera.director.aws;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.util.Map;

/**
 * Timeouts for different blocking operations in the AWS plugin. Each timeout
 * has a string name and is stored as a unit-less long value.
 */
public class AWSTimeouts {

  private final ImmutableMap<String, Long> timeouts;

  /**
   * Creates a new timeouts object from the given configuration. It is
   * expected that every value in the recursive tree of the configuration is
   * numeric. Each (recursive) key in the config serves as the key for a
   * timeout.
   *
   * @param config config holding timeouts
   * @throws IllegalArgumentException if any configuration value is non-numeric
   */
  public AWSTimeouts(Config config) {
    ImmutableMap.Builder<String, Long> b = ImmutableMap.builder();

    if (config != null) {
      for (Map.Entry<String,ConfigValue> e : config.entrySet()) {
        String key = e.getKey();
        ConfigValue value = e.getValue();
        switch (value.valueType()) {
          case NUMBER:
            long num =((Number) value.unwrapped()).longValue();
            if (num <= 0L) {
              throw new IllegalArgumentException("Timeout " + key + " is negative: " + value);
            }
            b.put(key, num);
            break;
          default:
            throw new IllegalArgumentException("Timeout " + key + " is not a number: " + value);
        }
      }
    }

    timeouts = b.build();
  }

  /**
   * Gets a timeout value.
   *
   * @param key timeout key
   * @return timeout value as an Optional
   */
  public Optional<Long> getTimeout(String key) {
    return Optional.fromNullable(timeouts.get(key));
  }
}
