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

package com.cloudera.director.aws.ec2;

import static com.cloudera.director.aws.ec2.EphemeralDeviceMappings.EphemeralDeviceMappingsConfigProperties.EphemeralDeviceMappingsConfigurationPropertyToken.CUSTOM_MAPPINGS_PATH;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.cloudera.director.aws.Configurations;
import com.cloudera.director.aws.common.PropertyResolvers;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.PropertyResolver;

/**
 * Maps an instance type to a list of block device mappings for ephemeral
 * storage. EC2 does not provide a means through the API to discover mappings.
 * This class looks up mappings using a built-in list and an override list that
 * the user can easily modify.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html">Amazon EC2 Instance Stor</a>
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/block-device-mapping-concepts.html">Block Device Mapping</a>
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "Guava"})
public class EphemeralDeviceMappings {

  private static final Logger LOG = LoggerFactory.getLogger(EphemeralDeviceMappings.class);

  public static class EphemeralDeviceMappingsConfigProperties extends DeviceMappingsConfigProperties {

    /**
     * Ephemeral device mappings configuration properties.
     */
    // Fully qualifying class name due to compiler bug
    @SuppressWarnings("PMD.UnnecessaryFullyQualifiedName")
    public enum EphemeralDeviceMappingsConfigurationPropertyToken
        implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {
      /**
       * Path for the custom device mappings file. Relative paths are based on
       * the plugin configuration directory.
       */
      CUSTOM_MAPPINGS_PATH(new SimpleConfigurationPropertyBuilder()
          .configKey("customMappingsPath")
          .name("Custom mappings path")
          .defaultDescription("The path for the custom ephemeral device mappings file. Relative " +
              "paths are based on the plugin configuration directory.")
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
      EphemeralDeviceMappingsConfigurationPropertyToken(
          ConfigurationProperty configurationProperty) {
        this.configurationProperty = configurationProperty;
      }

      @Override
      public ConfigurationProperty unwrap() {
        return configurationProperty;
      }
    }

    public static final String DEFAULT_DEVICE_NAME_PREFIX = "/dev/sd";

    /**
     * Ephemeral drives should be attached in the following range:
     * /dev/(sd|xvd)[b-y].
     */
    public static final char DEFAULT_RANGE_START_FOR_EPHEMERAL_DRIVES = 'b';

    public static final String DEFAULT_CUSTOM_MAPPINGS_PATH =
        "ec2.ephemeraldevicemappings.properties";

    private final File configurationDirectory;

    private String customMappingsPath = DEFAULT_CUSTOM_MAPPINGS_PATH;


    /**
     * Creates ephemeral device mappings config properties with the specified parameters.
     *
     * @param configuration            the configuration
     * @param configurationDirectory   the plugin configuration directory
     * @param cloudLocalizationContext the parent cloud localization context
     */
    public EphemeralDeviceMappingsConfigProperties(Configured configuration,
        File configurationDirectory,
        LocalizationContext cloudLocalizationContext) {
      super(Configurations.EPHEMERAL_DEVICE_MAPPINGS_SECTION, configuration, cloudLocalizationContext);
      this.configurationDirectory = configurationDirectory;
      LocalizationContext localizationContext = new ChildLocalizationContext(
          cloudLocalizationContext, Configurations.EPHEMERAL_DEVICE_MAPPINGS_SECTION);
      setCustomMappingsPath(configuration.getConfigurationValue(CUSTOM_MAPPINGS_PATH,
          localizationContext));
    }

    @Override
    public String getDefaultDeviceNamePrefix() {
      return DEFAULT_DEVICE_NAME_PREFIX;
    }

    @Override
    public char getDefaultRangeStart() {
      return DEFAULT_RANGE_START_FOR_EPHEMERAL_DRIVES;
    }

    public String getCustomMappingsPath() {
      return customMappingsPath;
    }

    File getCustomMappingsFile() {
      return getCustomMappingsFile(customMappingsPath);
    }

    File getCustomMappingsFile(String customMappingsPath) {
      File customMappingsPathFile = new File(customMappingsPath);
      return customMappingsPathFile.isAbsolute() ?
          customMappingsPathFile :
          new File(configurationDirectory, customMappingsPath);
    }

    public void setCustomMappingsPath(String customMappingsPath) {
      if (customMappingsPath != null) {
        LOG.info("Overriding customMappingsPath={} (default {})",
            customMappingsPath, DEFAULT_CUSTOM_MAPPINGS_PATH);
        checkArgument(getCustomMappingsFile(customMappingsPath).exists(),
            "Custom ephemeral device mappings path " +
                customMappingsPath + " does not exist");
        this.customMappingsPath = customMappingsPath;
      }
    }
  }

  public static class EphemeralDeviceMappingsConfig {

    public static final String BUILT_IN_LOCATION =
        "classpath:/com/cloudera/director/aws/ec2/ephemeraldevicemappings.properties";

    protected EphemeralDeviceMappingsConfigProperties
        ephemeralDeviceMappingsConfigProperties;

    public PropertyResolver ephemeralDeviceMappingsResolver() {
      try {
        return PropertyResolvers.newMultiResourcePropertyResolver(
            BUILT_IN_LOCATION,
            "file:" + ephemeralDeviceMappingsConfigProperties.getCustomMappingsFile().getAbsolutePath()
        );
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not load ephemeral device mappings", e);
      }
    }

    /**
     * Creates an ephemeral device mappings config with the specified parameters.
     *
     * @param ephemeralDeviceMappingsConfigProperties the config properties
     */
    public EphemeralDeviceMappingsConfig(
        EphemeralDeviceMappingsConfigProperties ephemeralDeviceMappingsConfigProperties) {
      this.ephemeralDeviceMappingsConfigProperties = ephemeralDeviceMappingsConfigProperties;
    }
  }

  EphemeralDeviceMappingsConfigProperties ephemeralDeviceMappingsConfigProperties;

  PropertyResolver ephemeralDeviceMappingsResolver;

  /**
   * Creates ephemeral device mappings with the specified parameters.
   *
   * @param configuration               the configuration
   * @param configurationDirectory      the plugin configuration directory
   * @param launcherLocalizationContext the parent launcher localization context
   */
  public EphemeralDeviceMappings(Configured configuration, File configurationDirectory,
      LocalizationContext launcherLocalizationContext) {
    this(new EphemeralDeviceMappingsConfigProperties(configuration, configurationDirectory,
        launcherLocalizationContext));
  }

  /**
   * Creates ephemeral device mappings with the specified parameters.
   *
   * @param ephemeralDeviceMappingsConfigProperties the config properties
   */
  public EphemeralDeviceMappings(
      EphemeralDeviceMappingsConfigProperties ephemeralDeviceMappingsConfigProperties) {
    this(ephemeralDeviceMappingsConfigProperties,
        new EphemeralDeviceMappingsConfig(ephemeralDeviceMappingsConfigProperties)
            .ephemeralDeviceMappingsResolver());
  }

  /**
   * Creates ephemeral device mappings with the specified parameters.
   *
   * @param ephemeralDeviceMappingsConfigProperties the config properties
   * @param ephemeralDeviceMappingsResolver         the ephemeral device mappings resolver
   */
  private EphemeralDeviceMappings(
      EphemeralDeviceMappingsConfigProperties ephemeralDeviceMappingsConfigProperties,
      PropertyResolver ephemeralDeviceMappingsResolver) {
    this.ephemeralDeviceMappingsConfigProperties = ephemeralDeviceMappingsConfigProperties;
    this.ephemeralDeviceMappingsResolver = ephemeralDeviceMappingsResolver;
  }

  private final DeviceNameUtils deviceNameUtils = new DeviceNameUtils();

  /**
   * Generates a list of block device mappings for all ephemeral drives for
   * the given instance type.
   *
   * @param instanceType       EC2 instance type
   * @param excludeDeviceNames set of device names that shouldn't be used for the block device mappings
   * @return list of block device mappings
   * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/block-device-mapping-concepts.html">Block Device Mapping</a>
   */
  public List<BlockDeviceMapping> getBlockDeviceMappings(String instanceType, Set<String> excludeDeviceNames) {
    checkNotNull(instanceType, "instanceType is null");

    Optional<Integer> optCount = getCount(instanceType);
    if (!optCount.isPresent()) {
      LOG.error("Unsupported instance type {}, add its ephemeral instance " +
          "volume count as a custom mapping; assuming zero", instanceType);
      return Collections.emptyList();
    }

    int count = optCount.get();
    if (count == 0) {
      return Collections.emptyList();
    }

    List<String> deviceNames = deviceNameUtils.getDeviceNames(
        ephemeralDeviceMappingsConfigProperties.getDeviceNamePrefix(),
        ephemeralDeviceMappingsConfigProperties.getRangeStart(),
        count, excludeDeviceNames);

    List<BlockDeviceMapping> result = Lists.newArrayListWithExpectedSize(count);
    int index = 0;
    for (String device : deviceNames) {
      result.add(new BlockDeviceMapping()
          .withDeviceName(device)
          .withVirtualName("ephemeral" + index));
      index += 1;
    }

    return result;
  }

  public List<BlockDeviceMapping> getBlockDeviceMappings(String instanceType) {
    return getBlockDeviceMappings(instanceType, Collections.emptySet());
  }

  private Optional<Integer> getCount(String instanceType) {
    return Optional.fromNullable(
        ephemeralDeviceMappingsResolver.getProperty(instanceType, Integer.class)
    );
  }

  /**
   * Gets a test instance of this class that uses only the given mapping.
   *
   * @param counts                      map of instance types to counts
   * @param launcherLocalizationContext the parent launcher localization context
   * @return new mapping object
   */
  public static EphemeralDeviceMappings getTestInstance(Map<String, Integer> counts,
      LocalizationContext launcherLocalizationContext) {
    Map<String, String> propertyMap =
        Maps.transformValues(counts, Functions.toStringFunction());
    PropertyResolver ephemeralDeviceMappingsResolver =
        PropertyResolvers.newMapPropertyResolver(propertyMap);
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    EphemeralDeviceMappingsConfigProperties ephemeralDeviceMappingsConfigProperties =
        new EphemeralDeviceMappingsConfigProperties(new SimpleConfiguration(),
            tempDir, launcherLocalizationContext);
    return new EphemeralDeviceMappings(ephemeralDeviceMappingsConfigProperties,
        ephemeralDeviceMappingsResolver);
  }
}
