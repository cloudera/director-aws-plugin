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

package com.cloudera.director.aws.ec2.ebs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.cloudera.director.aws.common.PropertyResolvers;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.PropertyResolver;

/**
 * A lookup mechanism for getting metadata about EBS volumes. AWS does not provide
 * a means through the API to discover this metadata. This class looks up metadata
 * using a built-in list and an override list that the user can easily modify.
 */
@SuppressWarnings("PMD.UnusedPrivateField")
public class EBSMetadata implements Function<String, EBSMetadata.EbsVolumeMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(EBSMetadata.class);

  /**
   * Contains metadata information for an EBS volume type.
   */
  @SuppressWarnings("Guava")
  public static class EbsVolumeMetadata {
    private final String type;
    private final int minSizeGiB;
    private final int maxSizeGiB;
    private final Optional<Integer> minIops;
    private final Optional<Integer> maxIops;

    public EbsVolumeMetadata(String type, int minSizeGiB, int maxSizeGiB) {
      this.type = Objects.requireNonNull(type, "type is null");
      this.minSizeGiB = minSizeGiB;
      this.maxSizeGiB = maxSizeGiB;
      this.minIops = Optional.absent();
      this.maxIops = Optional.absent();
    }

    public EbsVolumeMetadata(String type, int minSizeGiB, int maxSizeGiB, int minIops, int maxIops) {
      this.type = Objects.requireNonNull(type, "type is null");
      this.minSizeGiB = minSizeGiB;
      this.maxSizeGiB = maxSizeGiB;
      this.minIops = Optional.of(minIops);
      this.maxIops = Optional.of(maxIops);
    }

    public String getType() {
      return type;
    }

    public int getMinSizeGiB() {
      return minSizeGiB;
    }

    public int getMaxSizeGiB() {
      return maxSizeGiB;
    }

    public Optional<Integer> getMinIops() {
      return minIops;
    }

    public Optional<Integer> getMaxIops() {
      return maxIops;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EbsVolumeMetadata that = (EbsVolumeMetadata) o;

      if (minSizeGiB != that.minSizeGiB) return false;
      if (maxSizeGiB != that.maxSizeGiB) return false;
      if (!type.equals(that.type)) return false;
      if (!minIops.equals(that.minIops)) return false;
      return maxIops.equals(that.maxIops);
    }

    @Override
    public int hashCode() {
      int result = type.hashCode();
      result = 31 * result + minSizeGiB;
      result = 31 * result + maxSizeGiB;
      result = 31 * result + minIops.hashCode();
      result = 31 * result + maxIops.hashCode();
      return result;
    }
  }

  public static class EBSMetadataConfigProperties {

    /**
     * EBS metadata configuration properties.
     */
    // Fully qualifying class name due to compiler bug
    public enum EBSMetadataConfigurationPropertyToken
        implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

      /**
       * Path for the custom EBS metadata file. Relative paths are based on the
       * plugin configuration directory.
       */
      CUSTOM_EBS_METADATA_PATH(new SimpleConfigurationPropertyBuilder()
          .configKey("ebsMetadataPath")
          .name("Custom EBS metadata path")
          .defaultDescription("The path for the custom ebs metadata. Relative paths are based " +
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
      EBSMetadataConfigurationPropertyToken(
          ConfigurationProperty configurationProperty) {
        this.configurationProperty = configurationProperty;
      }

      @Override
      public ConfigurationProperty unwrap() {
        return configurationProperty;
      }
    }

    public static final String DEFAULT_CUSTOM_EBS_METADATA_PATH =
        "ec2.ebs.ebsmetadata.properties";

    private final File configurationDirectory;

    private String ebsMetadataPath = DEFAULT_CUSTOM_EBS_METADATA_PATH;

    /**
     * Creates EBS metadata config properties with the specified parameters.
     *
     * @param configuration            the configuration
     * @param configurationDirectory   the plugin configuration directory
     * @param cloudLocalizationContext the parent cloud localization context
     */
    public EBSMetadataConfigProperties(Configured configuration,
        File configurationDirectory,
        LocalizationContext cloudLocalizationContext) {
      this.configurationDirectory = configurationDirectory;
      LocalizationContext localizationContext = new ChildLocalizationContext(
          cloudLocalizationContext, "ebsMetadata");
      setCustomEbsMetadataPath(
          configuration.getConfigurationValue(EBSMetadataConfigurationPropertyToken.CUSTOM_EBS_METADATA_PATH,
              localizationContext));
    }

    public String getCustomEbsMetadataPath() {
      return ebsMetadataPath;
    }

    File getCustomEbsMetadataFile() {
      return getCustomEbsMetadataFile(ebsMetadataPath);
    }

    File getCustomEbsMetadataFile(String ebsMetadataPath) {
      File ebsMetadataPathFile = new File(ebsMetadataPath);
      return ebsMetadataPathFile.isAbsolute() ?
          ebsMetadataPathFile :
          new File(configurationDirectory, ebsMetadataPath);
    }

    public void setCustomEbsMetadataPath(String ebsMetadataPath) {
      if (ebsMetadataPath != null) {
        LOG.info("Overriding ebsMetadataPath={} (default {})",
            ebsMetadataPath, DEFAULT_CUSTOM_EBS_METADATA_PATH);
        checkArgument(getCustomEbsMetadataFile(ebsMetadataPath).exists(),
            "Custom EBS metadata path " +
                ebsMetadataPath + " does not exist");
        this.ebsMetadataPath = ebsMetadataPath;
      }
    }
  }

  public static class EBSMetadataConfig {

    public static final String BUILT_IN_LOCATION =
        "classpath:/com/cloudera/director/aws/ec2/ebs/ebsmetadata.properties";

    protected EBSMetadataConfigProperties ebsMetadataConfigProperties;

    public PropertyResolver ebsMetadataResolver() {
      try {
        return PropertyResolvers.newMultiResourcePropertyResolver(
            BUILT_IN_LOCATION,
            "file:" + ebsMetadataConfigProperties.getCustomEbsMetadataFile().getAbsolutePath()
        );
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not load custom ebs metadata", e);
      }
    }

    /**
     * Creates EBS metadata config with the specified parameters.
     *
     * @param ebsMetadataConfigProperties the config properties
     */
    public EBSMetadataConfig(EBSMetadataConfigProperties ebsMetadataConfigProperties) {
      this.ebsMetadataConfigProperties = ebsMetadataConfigProperties;
    }
  }

  private final EBSMetadataConfigProperties ebsMetadataConfigProperties;

  private final PropertyResolver ebsMetadataResolver;

  /**
   * Creates EBS metadata with the specified parameters.
   *
   * @param configuration          the configuration
   * @param configurationDirectory the plugin configuration directory
   * @param localizationContext    the localization context
   */
  public EBSMetadata(Configured configuration, File configurationDirectory,
      LocalizationContext localizationContext) {
    this(new EBSMetadataConfigProperties(configuration, configurationDirectory,
        localizationContext));
  }

  /**
   * Creates EBS metadata with the specified parameters.
   *
   * @param ebsMetadataConfigProperties the config properties
   */
  public EBSMetadata(EBSMetadataConfigProperties ebsMetadataConfigProperties) {
    this(ebsMetadataConfigProperties, new EBSMetadataConfig(ebsMetadataConfigProperties).ebsMetadataResolver());
  }

  /**
   * Creates EBS metadata with the specified parameters.
   *
   * @param ebsMetadataConfigProperties the config properties
   * @param ebsMetadataResolver         the ebs metadata resolver
   */
  private EBSMetadata(EBSMetadataConfigProperties ebsMetadataConfigProperties,
      PropertyResolver ebsMetadataResolver) {
    this.ebsMetadataConfigProperties = ebsMetadataConfigProperties;
    this.ebsMetadataResolver = ebsMetadataResolver;
  }

  /**
   * Gets the metadata for an EBS volume type. A custom definition overrides a
   * built-in definition.
   *
   * @param volumeType ebs volume type
   * @return metadata associated with the volume type
   * @throws NullPointerException  if no metadata could be found for a volume type
   * @throws IllegalStateException if the metadata for a volume type has invalid format
   */
  @Override
  public EbsVolumeMetadata apply(String volumeType) {
    String strCapacityRange = ebsMetadataResolver.getProperty(volumeType);
    Objects.requireNonNull(strCapacityRange, String.format("Could not get metadata for volume type %s", volumeType));

    Range capacityRange = Range.resolveRange(volumeType, strCapacityRange);
    int minSizeGiB = capacityRange.getMin();
    int maxSizeGiB = capacityRange.getMax();

    if (volumeType.equals("io1")) {
      String key = "io1-iops";
      String strIopsMetadata = ebsMetadataResolver.getProperty(key);
      Objects.requireNonNull(strIopsMetadata, String.format("Could not get metadata for %s", key));

      Range iopsRange = Range.resolveRange(key, strIopsMetadata);
      return new EbsVolumeMetadata(volumeType, minSizeGiB, maxSizeGiB, iopsRange.getMin(), iopsRange.getMax());
    }

    return new EbsVolumeMetadata(volumeType, minSizeGiB, maxSizeGiB);
  }

  private static class Range {
    private int min;
    private int max;

    private Range(int min, int max) {
      this.min = min;
      this.max = max;
    }

    public int getMin() {
      return min;
    }

    public int getMax() {
      return max;
    }

    static Range resolveRange(String key, String rangeStr) {

      // range string is expected to be in the form "minValue-maxValue"

      String[] parts = rangeStr.trim().split("-");
      checkState(parts.length == 2, String.format("invalid format for %s=%s, expecting " +
          "exactly 2 parts for the value", key, rangeStr));

      int min, max;
      try {
        min = Integer.parseInt(parts[0]);
      } catch (NumberFormatException ex) {
        throw new IllegalStateException(
            String.format("invalid format for %s=%s, %s should be an integer", key, rangeStr, parts[0])
        );
      }

      try {
        max = Integer.parseInt(parts[1]);
      } catch (NumberFormatException ex) {
        throw new IllegalStateException(
            String.format("invalid format for %s=%s, %s should be an integer", key, rangeStr, parts[1])
        );
      }

      checkState(min > 0, String.format("invalid format for %s=%s, %d should " +
          "be a positive integer", key, rangeStr, min));

      checkState(max > 0, String.format("invalid format for %s=%s, %d should " +
          "be a positive integer", key, rangeStr, max));

      checkState(max >= min, String.format("invalid format for %s=%s, maximum size %d should " +
          "be greater than or equal to minimum size %d", key, rangeStr, max, min));

      return new Range(min, max);
    }
  }


  /**
   * Gets an instance of this class that uses only the given ebs metadata.
   *
   * @param metadata            map of ebs volume type to it's associated metadata
   * @param localizationContext the localization context
   * @return new ebs metadata object
   */
  public static EBSMetadata getDefaultInstance(
      final Map<String, String> metadata, LocalizationContext localizationContext) {
    PropertyResolver ebsMetadataResolver =
        PropertyResolvers.newMapPropertyResolver(metadata);
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    EBSMetadataConfigProperties ebsMetadataConfigProperties =
        new EBSMetadataConfigProperties(new SimpleConfiguration(), tempDir, localizationContext);
    return new EBSMetadata(ebsMetadataConfigProperties, ebsMetadataResolver);
  }
}
