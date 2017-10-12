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

package com.cloudera.director.aws.ec2;

import static com.cloudera.director.aws.ec2.DeviceMappingsConfigProperties.DeviceMappingsConfigurationPropertyToken.DEVICE_NAME_PREFIX;
import static com.cloudera.director.aws.ec2.DeviceMappingsConfigProperties.DeviceMappingsConfigurationPropertyToken.RANGE_START;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common configuration properties for device mappings.
 */
public abstract class DeviceMappingsConfigProperties {

  private static final Logger LOG = LoggerFactory.getLogger(DeviceMappingsConfigProperties.class);

  public abstract String getDefaultDeviceNamePrefix();
  public abstract char getDefaultRangeStart();

  private String configSection;
  private String deviceNamePrefix = getDefaultDeviceNamePrefix();
  private char rangeStart = getDefaultRangeStart();

  /**
   *  Device mappings configuration properties.
   */
  // Fully qualifying class name due to compiler bug
  public static enum DeviceMappingsConfigurationPropertyToken
      implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

    /**
     * The device name prefix for device.
     */
    DEVICE_NAME_PREFIX(new SimpleConfigurationPropertyBuilder()
        .configKey("deviceNamePrefix")
        .name("Device name prefix")
        .defaultDescription("The device name prefix for mapped devices.")
        .build()),

    /**
     * The range start for attaching the device.
     */
    RANGE_START(new SimpleConfigurationPropertyBuilder()
        .configKey("rangeStart")
        .name("Range start")
        .defaultDescription("The first character suffix in the range of device names to be generated.")
        .build());

    /**
     * The configuration property.
     */
    private final ConfigurationProperty configurationProperty;

    /**
     * Creates a configuration property token with the specified parameters.
     *
     * @param configurationProperty the configuration property
     */
    DeviceMappingsConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    @Override
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }
  }

  /**
   * Creates device mappings config properties with the specified parameters.
   *
   * @param configSection            the plugin configuration section
   * @param configuration            the configuration
   * @param cloudLocalizationContext the parent cloud localization context
   */
  public DeviceMappingsConfigProperties(String configSection, Configured configuration,
                                        LocalizationContext cloudLocalizationContext) {
    LocalizationContext localizationContext = new ChildLocalizationContext(
        cloudLocalizationContext, configSection);
    this.configSection = configSection;
    setDeviceNamePrefix(configuration.getConfigurationValue(DEVICE_NAME_PREFIX,
        localizationContext));
    setRangeStart(configuration.getConfigurationValue(RANGE_START,
        localizationContext));
  }

  public String getDeviceNamePrefix() {
    return deviceNamePrefix;
  }

  public char getRangeStart() {
    return rangeStart;
  }

  public void setDeviceNamePrefix(String deviceNamePrefix) {
    if (deviceNamePrefix != null) {
      LOG.info("Overriding deviceNamePrefix={} (default {}) under {} section", deviceNamePrefix,
          getDefaultDeviceNamePrefix(), configSection);
      this.deviceNamePrefix = deviceNamePrefix;
    }
  }

  public void setRangeStart(String rangeStart) {
    if (rangeStart != null) {
      if (rangeStart.length() != 1) {
        throw new IllegalArgumentException("rangeStart must be a single character");
      }
      char c = rangeStart.charAt(0);
      LOG.info("Overriding rangeStart={} (default {}) under {} section", c,
          getDefaultRangeStart(), configSection);
      this.rangeStart = c;
    }
  }
}
