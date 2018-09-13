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

package com.cloudera.director.aws.ec2.ebs;

import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.cloudera.director.aws.Configurations;
import com.cloudera.director.aws.ec2.DeviceMappingsConfigProperties;
import com.cloudera.director.aws.ec2.DeviceNameUtils;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains functions to retrieve block device mappings and device names for EBS storage.
 */
public class EBSDeviceMappings {

  private final DeviceNameUtils deviceNameUtils = new DeviceNameUtils();

  static final String DEFAULT_EBS_DEVICE_NAME_PREFIX = "/dev/sd";
  static final char DEFAULT_EBS_DEVICE_NAME_START_CHAR = 'f';

  public static class EbsDeviceMappingsConfigProperties extends DeviceMappingsConfigProperties {

    /**
     * Creates EBS device mappings config properties with the specified parameters.
     *
     * @param configSection            the plugin configuration section
     * @param configuration            the configuration
     * @param cloudLocalizationContext the parent cloud localization context
     */
    public EbsDeviceMappingsConfigProperties(String configSection, Configured configuration,
        LocalizationContext cloudLocalizationContext) {
      super(configSection, configuration, cloudLocalizationContext);
    }

    @Override
    public String getDefaultDeviceNamePrefix() {
      return DEFAULT_EBS_DEVICE_NAME_PREFIX;
    }

    @Override
    public char getDefaultRangeStart() {
      return DEFAULT_EBS_DEVICE_NAME_START_CHAR;
    }
  }

  private final EbsDeviceMappingsConfigProperties ebsDeviceMappingsConfigProperties;

  /**
   * Creates EBS device mappings with the specified configuration.
   *
   * @param configuration            the configuration
   * @param cloudLocalizationContext the parent cloud localization context
   */
  public EBSDeviceMappings(Configured configuration, LocalizationContext cloudLocalizationContext) {
    this.ebsDeviceMappingsConfigProperties = new EbsDeviceMappingsConfigProperties(
        Configurations.EBS_DEVICE_MAPPINGS_SECTION, configuration, cloudLocalizationContext);
  }

  /**
   * Gets a list of EBS block device mappings with the specified parameters.
   *
   * @param template     EC2InstanceTemplate which contains information about
   *                     the EBS volumes requested
   * @param excludeDeviceNames set device names that shouldn't be used for the block device mappings
   * @return set of EBS BlockDeviceMapping
   */
  @SuppressWarnings("Guava")
  public List<BlockDeviceMapping> getBlockDeviceMappings(EC2InstanceTemplate template,
                                                         Set<String> excludeDeviceNames) {
    List<SystemDisk> systemDisks = template.getSystemDisks();

    int templateDiskCount = template.getEbsVolumeCount();
    int systemDiskCount = systemDisks.size();
    int totalVolumeCount = templateDiskCount + systemDiskCount;

    List<String> deviceNames = getDeviceNames(totalVolumeCount, excludeDeviceNames);

    List<BlockDeviceMapping> mappings = Lists.newArrayList();

    for (int i = 0; i < systemDisks.size(); i++) {
      String deviceName = deviceNames.get(i);
      SystemDisk systemDisk = systemDisks.get(i);
      mappings.add(systemDisk.toBlockDeviceMapping(deviceName));
    }

    for (int i = 0; i < templateDiskCount; i++) {
      String deviceName = deviceNames.get(i + systemDiskCount);

      EbsBlockDevice ebs = new EbsBlockDevice()
          .withVolumeType(template.getEbsVolumeType())
          .withVolumeSize(template.getEbsVolumeSizeGiB())
          .withEncrypted(template.isEnableEbsEncryption())
          .withDeleteOnTermination(true);

      Optional<Integer> iops = template.getEbsIops();
      Optional<String> kmsKeyid = template.getEbsKmsKeyId();

      if (iops.isPresent()) {
        ebs = ebs.withIops(iops.get());
      }
      if (kmsKeyid.isPresent()) {
        ebs = ebs.withKmsKeyId(kmsKeyid.get());
      }

      BlockDeviceMapping mapping = new BlockDeviceMapping()
          .withDeviceName(deviceName)
          .withEbs(ebs);

      mappings.add(mapping);
    }

    return mappings;
  }

  List<String> getDeviceNames(int count, Set<String> filter) {
    return deviceNameUtils.getDeviceNames(
        ebsDeviceMappingsConfigProperties.getDeviceNamePrefix(),
        ebsDeviceMappingsConfigProperties.getRangeStart(),
        count, filter
    );
  }


  /**
   * Gets an instance of this class that uses only the given EBS device mapping configuration.
   *
   * @param configuration       EBS device mapping configuration
   * @param localizationContext the localization context
   * @return new EBS metadata object
   */
  public static EBSDeviceMappings getDefaultInstance(
      final Map<String, String> configuration, LocalizationContext localizationContext) {
    return new EBSDeviceMappings(new SimpleConfiguration(configuration), localizationContext);
  }
}
