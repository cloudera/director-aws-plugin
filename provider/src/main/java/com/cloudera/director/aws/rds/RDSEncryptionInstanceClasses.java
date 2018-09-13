// (c) Copyright 2016 Cloudera, Inc.
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
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.PropertyResolver;

/**
 * A lookup mechanism for RDS encryption instance classes. This class looks up
 * whether an instance class supports storage encryption using a built-in list
 * and an override list that the user can easily modify.
 */
public class RDSEncryptionInstanceClasses implements Function<String, Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(RDSEncryptionInstanceClasses.class);

  /**
   * RDS encryption instance classes configuration properties.
   */
  public enum ConfigurationPropertyToken
      implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

    /**
     * Path for the custom RDS encryption instance classes file. Relative
     * paths are based on the plugin configuration directory.
     */
    CUSTOM_ENCRYPTION_INSTANCE_CLASSES_PATH(new SimpleConfigurationPropertyBuilder()
        .configKey("customEncryptionInstanceClassesPath")
        .name("Custom encryption instance classes path")
        .defaultDescription("The path for the custom list of RDS encryption instance classes. " +
            "Relative paths are based on the plugin configuration directory.")
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
    ConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    @Override
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }
  }

  public static class RDSEncryptionInstanceClassesConfigProperties {

    public static final String DEFAULT_CUSTOM_ENCRYPTION_INSTANCE_CLASSES_PATH =
        "rds.encryptioninstanceclasses.properties";

    private final File configurationDirectory;

    private String customEncryptionInstanceClassesPath =
        DEFAULT_CUSTOM_ENCRYPTION_INSTANCE_CLASSES_PATH;

    /**
     * Creates RDS encryption instance classes config properties with the
     * specified parameters.
     *
     * @param configuration            the configuration
     * @param configurationDirectory   the plugin configuration directory
     * @param cloudLocalizationContext the parent cloud localization context
     */
    public RDSEncryptionInstanceClassesConfigProperties(Configured configuration,
        File configurationDirectory, LocalizationContext cloudLocalizationContext) {
      this.configurationDirectory = configurationDirectory;
      LocalizationContext localizationContext = new ChildLocalizationContext(
          cloudLocalizationContext, "rdsEncryptionInstanceClasses");
      setCustomEncryptionInstanceClassesPath(
          configuration.getConfigurationValue(ConfigurationPropertyToken.CUSTOM_ENCRYPTION_INSTANCE_CLASSES_PATH,
              localizationContext));
    }

    @VisibleForTesting
    String getCustomEncryptionInstanceClassesPath() {
      return customEncryptionInstanceClassesPath;
    }

    File getCustomEncryptionInstanceClassesFile() {
      return getCustomEncryptionInstanceClassesFile(customEncryptionInstanceClassesPath);
    }

    private File getCustomEncryptionInstanceClassesFile(String customEncryptionInstanceClassesPath) {
      File customEncryptionInstanceClassesPathFile = new File(customEncryptionInstanceClassesPath);
      return customEncryptionInstanceClassesPathFile.isAbsolute() ?
          customEncryptionInstanceClassesPathFile :
          new File(configurationDirectory, customEncryptionInstanceClassesPath);
    }

    public final void setCustomEncryptionInstanceClassesPath(String customEncryptionInstanceClassesPath) {
      if (customEncryptionInstanceClassesPath != null) {
        LOG.info("Overriding customEncryptionInstanceClassesPath={} (default {})",
            customEncryptionInstanceClassesPath,
            DEFAULT_CUSTOM_ENCRYPTION_INSTANCE_CLASSES_PATH);
        checkArgument(getCustomEncryptionInstanceClassesFile(customEncryptionInstanceClassesPath).exists(),
            "Custom RDS encryption instance classes path " +
                customEncryptionInstanceClassesPath + " does not exist");
        this.customEncryptionInstanceClassesPath = customEncryptionInstanceClassesPath;
      }
    }
  }

  private static final String BUILT_IN_LOCATION =
      "classpath:/com/cloudera/director/aws/rds/encryptioninstanceclasses.properties";

  private static PropertyResolver getResolver(RDSEncryptionInstanceClassesConfigProperties configProperties) {
    try {
      return PropertyResolvers.newMultiResourcePropertyResolver(
          BUILT_IN_LOCATION,
          "file:" + configProperties.getCustomEncryptionInstanceClassesFile().getAbsolutePath()
      );
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not load custom encryption instance classes", e);
    }
  }

  private final RDSEncryptionInstanceClassesConfigProperties rdsEncryptionInstanceClassesConfigProperties;
  private final PropertyResolver rdsEncryptionInstanceClassesResolver;

  /**
   * Creates RDS encryption instance classes with the specified parameters.
   *
   * @param configuration          the configuration
   * @param configurationDirectory the plugin configuration directory
   * @param localizationContext    the localization context
   */
  public RDSEncryptionInstanceClasses(Configured configuration, File configurationDirectory,
      LocalizationContext localizationContext) {
    this.rdsEncryptionInstanceClassesConfigProperties =
        new RDSEncryptionInstanceClassesConfigProperties(configuration, configurationDirectory,
            localizationContext);
    rdsEncryptionInstanceClassesResolver =
        getResolver(this.rdsEncryptionInstanceClassesConfigProperties);
  }

  /**
   * Creates RDS encryption instance classes with the specified parameters.
   *
   * @param rdsEncryptionInstanceClassesConfigProperties the config properties
   * @param rdsEncryptionInstanceClassesResolver         the RDS encryption instance classes resolver
   */
  private RDSEncryptionInstanceClasses(RDSEncryptionInstanceClassesConfigProperties rdsEncryptionInstanceClassesConfigProperties,
      PropertyResolver rdsEncryptionInstanceClassesResolver) {
    this.rdsEncryptionInstanceClassesConfigProperties =
        rdsEncryptionInstanceClassesConfigProperties;
    this.rdsEncryptionInstanceClassesResolver = rdsEncryptionInstanceClassesResolver;
  }


  /**
   * Determines if the given instance class supports storage encryption. A
   * custom definition overrides a built-in definition.
   *
   * @param instanceClass instance class
   * @return true if instance class supports encryption
   */
  @Nullable
  @Override
  public Boolean apply(String instanceClass) {
    return Boolean.valueOf(rdsEncryptionInstanceClassesResolver.getProperty(instanceClass, "false"));
  }

  /**
   * Gets a test instance of this class that uses only the given encryption
   * instance classes.
   *
   * @param encryptionInstanceClasses list of instance classes that support storage encryption
   * @param localizationContext       the localization context
   * @return new encryption instance classes object
   */
  public static RDSEncryptionInstanceClasses getTestInstance(
      final List<String> encryptionInstanceClasses, LocalizationContext localizationContext) {
    Map<String, String> encryptionInstanceClassesMap = Maps.newHashMap();
    for (String instanceClass : encryptionInstanceClasses) {
      encryptionInstanceClassesMap.put(instanceClass, "true");
    }
    PropertyResolver resolver =
        PropertyResolvers.newMapPropertyResolver(encryptionInstanceClassesMap);

    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();

    RDSEncryptionInstanceClassesConfigProperties configProperties =
        new RDSEncryptionInstanceClassesConfigProperties(new SimpleConfiguration(), tempDir,
            localizationContext);
    return new RDSEncryptionInstanceClasses(configProperties, resolver);
  }
}
