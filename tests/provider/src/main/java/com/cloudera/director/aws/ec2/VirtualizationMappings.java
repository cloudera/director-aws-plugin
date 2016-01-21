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

import static com.cloudera.director.aws.ec2.VirtualizationMappings.VirtualizationMappingsConfigProperties.VirtualizationMappingsConfigurationPropertyToken.CUSTOM_MAPPINGS_PATH;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.cloudera.director.aws.common.PropertyResolvers;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.PropertyResolver;

/**
 * Maps a virtualization type to the instance types that support it. EC2 does
 * not provide a means through the API to discover mappings. This class looks up
 * mappings using a built-in list and an override list that the user can easily
 * modify.
 */
public class VirtualizationMappings implements Function<String, List<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(VirtualizationMappings.class);

  private static final Splitter SPLITTER = Splitter.on(',').trimResults();
  private static final Joiner JOINER = Joiner.on(',');

  public static class VirtualizationMappingsConfigProperties {

    /**
     * Virtualization mappings configuration properties.
     */
    // Fully qualifying class name due to compiler bug
    public static enum VirtualizationMappingsConfigurationPropertyToken
        implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

      /**
       * Path for the custom virtualization mappings file. Relative paths are
       * based on the plugin configuration directory.
       */
      CUSTOM_MAPPINGS_PATH(new SimpleConfigurationPropertyBuilder()
          .configKey("customMappingsPath")
          .name("Custom mappings path")
          .defaultDescription("The path for the custom virtualization mappings file. Relative " +
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
      private VirtualizationMappingsConfigurationPropertyToken(
          ConfigurationProperty configurationProperty) {
        this.configurationProperty = configurationProperty;
      }

      @Override
      public ConfigurationProperty unwrap() {
        return configurationProperty;
      }
    }

    public static final String DEFAULT_CUSTOM_MAPPINGS_PATH =
        "ec2.virtualizationmappings.properties";

    private final File configurationDirectory;

    private String customMappingsPath = DEFAULT_CUSTOM_MAPPINGS_PATH;

    /**
     * Creates virtualization mappings config properties with the specified parameters.
     *
     * @param configuration            the configuration
     * @param configurationDirectory   the plugin configuration directory
     * @param cloudLocalizationContext the parent cloud localization context
     */
    public VirtualizationMappingsConfigProperties(Configured configuration,
        File configurationDirectory,
        LocalizationContext cloudLocalizationContext) {
      this.configurationDirectory = configurationDirectory;
      LocalizationContext localizationContext = new ChildLocalizationContext(
          cloudLocalizationContext, "virtualizationMappings");
      setCustomMappingsPath(
          configuration.getConfigurationValue(CUSTOM_MAPPINGS_PATH, localizationContext));
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
        LOG.info("Overriding customMappingsPath=%s (default {})",
            customMappingsPath, DEFAULT_CUSTOM_MAPPINGS_PATH);
        checkArgument(getCustomMappingsFile(customMappingsPath).exists(),
                      "Custom virtualization mappings path " +
                      customMappingsPath + " does not exist");
        this.customMappingsPath = customMappingsPath;
      }
    }
  }

  public static class VirtualizationMappingsConfig {

    public static final String BUILT_IN_LOCATION =
        "classpath:/com/cloudera/director/aws/ec2/virtualizationmappings.properties";

    protected VirtualizationMappingsConfigProperties
        virtualizationMappingsConfigProperties;

    public PropertyResolver virtualizationMappingsResolver() {
      try {
        return PropertyResolvers.newMultiResourcePropertyResolver(
            BUILT_IN_LOCATION,
            "file:" + virtualizationMappingsConfigProperties.getCustomMappingsFile().getAbsolutePath()
        );
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not load custom virtualization mappings", e);
      }
    }

    /**
     * Creates an virtualization mappings config with the specified parameters.
     *
     * @param virtualizationMappingsConfigProperties the config properties
     */
    public VirtualizationMappingsConfig(
        VirtualizationMappingsConfigProperties virtualizationMappingsConfigProperties) {
      this.virtualizationMappingsConfigProperties = virtualizationMappingsConfigProperties;
    }
  }

  VirtualizationMappingsConfigProperties virtualizationMappingsConfigProperties;

  PropertyResolver virtualizationMappingsResolver;

  /**
   * Creates virtualization mappings with the specified parameters.
   *
   * @param configuration               the configuration
   * @param configurationDirectory      the plugin configuration directory
   * @param localizationContext         the localization context
   */
  public VirtualizationMappings(Configured configuration, File configurationDirectory,
                                LocalizationContext localizationContext) {
    this(new VirtualizationMappingsConfigProperties(configuration, configurationDirectory,
                                                    localizationContext));
  }

  /**
   * Creates virtualization mappings with the specified parameters.
   *
   * @param virtualizationMappingsConfigProperties the config properties
   */
  public VirtualizationMappings(VirtualizationMappingsConfigProperties virtualizationMappingsConfigProperties) {
    this(virtualizationMappingsConfigProperties, new VirtualizationMappingsConfig(virtualizationMappingsConfigProperties).virtualizationMappingsResolver());
  }

  /**
   * Creates virtualization mappings with the specified parameters.
   *
   * @param virtualizationMappingsConfigProperties the config properties
   * @param virtualizationMappingsResolver         the virtualization mappings resolver
   */
  private VirtualizationMappings(VirtualizationMappingsConfigProperties virtualizationMappingsConfigProperties,
                                 PropertyResolver virtualizationMappingsResolver) {
    this.virtualizationMappingsConfigProperties = virtualizationMappingsConfigProperties;
    this.virtualizationMappingsResolver = virtualizationMappingsResolver;
  }

  /**
   * Gets the instance types that support the given virtualization type.
   *
   * @param virtualizationType virtualization type
   * @return list of instance types
   * @throws IllegalArgumentException if the virtualization type is unknown
   */
  @Nonnull
  public List<String> apply(String virtualizationType) {
    checkNotNull(virtualizationType, "virtualizationType is null");
    String instanceTypeList =
        virtualizationMappingsResolver.getProperty(virtualizationType);
    if (instanceTypeList == null) {
      throw new IllegalArgumentException("Unknown virtualization type " +
          virtualizationType);
    }
    return SPLITTER.splitToList(instanceTypeList);
  }

  /**
   * Gets a test instance of this class that uses only the given mapping.
   *
   * @param instanceTypes       map of virtualization types to instance types
   * @param localizationContext the localization context
   * @return new mapping object
   */
  public static VirtualizationMappings getTestInstance(
      final Map<String, List<String>> instanceTypes, LocalizationContext localizationContext) {
    Map<String, String> propertyMap =
        Maps.transformValues(instanceTypes,
            new Function<List<String>, String>() {
              @Override
              public String apply(@Nonnull List<String> input) {
                return JOINER.join(input);
              }
            });
    PropertyResolver virtualizationMappingsResolver =
        PropertyResolvers.newMapPropertyResolver(propertyMap);
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    VirtualizationMappingsConfigProperties virtualizationMappingsConfigProperties =
        new VirtualizationMappingsConfigProperties(new SimpleConfiguration(),
            tempDir, localizationContext);
    return new VirtualizationMappings(virtualizationMappingsConfigProperties,
        virtualizationMappingsResolver);
  }
}
