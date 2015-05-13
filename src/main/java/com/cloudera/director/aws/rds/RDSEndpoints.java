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

package com.cloudera.director.aws.rds;

import static com.google.common.base.Preconditions.checkArgument;

import com.cloudera.director.aws.common.PropertyResolvers;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.google.common.base.Function;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.PropertyResolver;

/**
 * A lookup mechanism for RDS endpoints. Unlike EC2, RDS does not provide a
 * means through the API to discover endpoints. This class looks up endpoints
 * using a built-in list and an override list that the user can easily modify.
 */
@SuppressWarnings("PMD.UnusedPrivateField")
public class RDSEndpoints implements Function<String, String> {

  private static final Logger LOG = LoggerFactory.getLogger(RDSEndpoints.class);

  public static class RDSEndpointsConfigProperties {

    /**
     * RDS endpoints configuration properties.
     */
    // Fully qualifying class name due to compiler bug
    public static enum RDSEndpointsConfigurationPropertyToken
        implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

      /**
       * Path for the custom RDS endpoints file. Relative paths are based on the
       * plugin configuration directory.
       */
      CUSTOM_ENDPOINTS_PATH(new SimpleConfigurationPropertyBuilder()
          .configKey("customEndpointsPath")
          .name("Custom endpoints path")
          .defaultDescription("The path for the custom RDS endpoints. Relative paths are based " +
                              "on the plugin configuration directory.")
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
      private RDSEndpointsConfigurationPropertyToken(
          ConfigurationProperty configurationProperty) {
        this.configurationProperty = configurationProperty;
      }

      @Override
      public ConfigurationProperty unwrap() {
        return configurationProperty;
      }
    }

    public static final String DEFAULT_CUSTOM_ENDPOINTS_PATH =
        "rds.endpoints.properties";

    private final File configurationDirectory;

    private String customEndpointsPath = DEFAULT_CUSTOM_ENDPOINTS_PATH;

    /**
     * Creates RDS endpoints config properties with the specified parameters.
     *
     * @param configuration            the configuration
     * @param configurationDirectory   the plugin configuration directory
     * @param cloudLocalizationContext the parent cloud localization context
     */
    public RDSEndpointsConfigProperties(Configured configuration,
        File configurationDirectory,
        LocalizationContext cloudLocalizationContext) {
      this.configurationDirectory = configurationDirectory;
      LocalizationContext localizationContext = new ChildLocalizationContext(
          cloudLocalizationContext, "rdsEndpoints");
      setCustomEndpointsPath(
          configuration.getConfigurationValue(RDSEndpointsConfigurationPropertyToken.CUSTOM_ENDPOINTS_PATH,
                                              localizationContext));
    }

    public String getCustomEndpointsPath() {
      return customEndpointsPath;
    }

    File getCustomEndpointsFile() {
      return getCustomEndpointsFile(customEndpointsPath);
    }

    File getCustomEndpointsFile(String customEndpointsPath) {
      File customEndpointsPathFile = new File(customEndpointsPath);
      return customEndpointsPathFile.isAbsolute() ?
             customEndpointsPathFile :
             new File(configurationDirectory, customEndpointsPath);
    }

    public void setCustomEndpointsPath(String customEndpointsPath) {
      if (customEndpointsPath != null) {
        LOG.info("Overriding customEndpointsPath={} (default {})",
                               customEndpointsPath, DEFAULT_CUSTOM_ENDPOINTS_PATH);
        checkArgument(getCustomEndpointsFile(customEndpointsPath).exists(),
                      "Custom RDS endpoints path " +
                      customEndpointsPath + " does not exist");
        this.customEndpointsPath = customEndpointsPath;
      }
    }
  }

  public static class RDSEndpointsConfig {

    public static final String BUILT_IN_LOCATION =
        "classpath:/com/cloudera/director/aws/rds/endpoints.properties";

    protected RDSEndpointsConfigProperties rdsEndpointsConfigProperties;

    public PropertyResolver rdsEndpointsResolver() {
      try {
        return PropertyResolvers.newMultiResourcePropertyResolver(
            BUILT_IN_LOCATION,
            "file:" + rdsEndpointsConfigProperties.getCustomEndpointsFile().getAbsolutePath()
        );
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not load custom endpoints", e);
      }
    }

    /**
     * Creates an RDS endpoints config with the specified parameters.
     *
     * @param rdsEndpointsConfigProperties the config properties
     */
    public RDSEndpointsConfig(RDSEndpointsConfigProperties rdsEndpointsConfigProperties) {
      this.rdsEndpointsConfigProperties = rdsEndpointsConfigProperties;
    }
  }

  private final RDSEndpointsConfigProperties rdsEndpointsConfigProperties;

  private final PropertyResolver rdsEndpointsResolver;

  /**
   * Creates RDS endpoints with the specified parameters.
   *
   * @param configuration               the configuration
   * @param configurationDirectory      the plugin configuration directory
   * @param localizationContext         the localization context
   */
  public RDSEndpoints(Configured configuration, File configurationDirectory,
                      LocalizationContext localizationContext) {
    this(new RDSEndpointsConfigProperties(configuration, configurationDirectory,
                                          localizationContext));
  }

  /**
   * Creates RDS endpoints with the specified parameters.
   *
   * @param rdsEndpointsConfigProperties the config properties
   */
  public RDSEndpoints(RDSEndpointsConfigProperties rdsEndpointsConfigProperties) {
    this(rdsEndpointsConfigProperties, new RDSEndpointsConfig(rdsEndpointsConfigProperties).rdsEndpointsResolver());
  }

  /**
   * Creates RDS endpoints with the specified parameters.
   *
   * @param rdsEndpointsConfigProperties the config properties
   * @param rdsEndpointsResolver         the RDS endpoints resolver
   */
  private RDSEndpoints(RDSEndpointsConfigProperties rdsEndpointsConfigProperties,
                       PropertyResolver rdsEndpointsResolver) {
    this.rdsEndpointsConfigProperties = rdsEndpointsConfigProperties;
    this.rdsEndpointsResolver = rdsEndpointsResolver;
  }


  /**
   * Gets the endpoint for the given region. A custom definition overrides a
   * built-in definition.
   *
   * @param regionName region name
   * @return region endpoint if known
   */
  @Nullable @Override
  public String apply(String regionName) {
    return rdsEndpointsResolver.getProperty(regionName);
  }

  /**
   * Gets a test instance of this class that uses only the given endpoints.
   *
   * @param endpoints           map of RDS regions to endpoint URLs
   * @param localizationContext the localization context
   * @return new endpoints object
   */
  public static RDSEndpoints getTestInstance(
      final Map<String, String> endpoints, LocalizationContext localizationContext) {
    PropertyResolver rdsEndpointsResolver =
        PropertyResolvers.newMapPropertyResolver(endpoints);
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    RDSEndpointsConfigProperties rdsEndpointsConfigProperties =
        new RDSEndpointsConfigProperties(new SimpleConfiguration(), tempDir, localizationContext);
    return new RDSEndpoints(rdsEndpointsConfigProperties, rdsEndpointsResolver);
  }
}
