// (c) Copyright 2018 Cloudera, Inc.
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
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This class represents an extra EBS volume that will get mounted. This is a temporary
 * solution to allowing heterogeneous EBS volumes and may be removed in the future.
 */
public class SystemDisk {

  private final String volumeType;
  private final int volumeSize;
  private final boolean enableEncryption;
  private final String kmsKeyId;

  /**
   * Constructor.
   *
   * @param volumeType the ebs volume type
   * @param volumeSize the ebs volume size in GiB
   * @param enableEncryption whether to enable ebs encryption
   * @param kmsKeyId optional encryption KMS key id
   */
  @JsonCreator
  public SystemDisk(
      @JsonProperty("volumeType") String volumeType,
      @JsonProperty("volumeSize") Integer volumeSize,
      @JsonProperty("enableEncryption") Boolean enableEncryption,
      @JsonProperty("kmsKeyId") String kmsKeyId) {
    this.volumeType = Objects.requireNonNull(volumeType);
    this.volumeSize = Objects.requireNonNull(volumeSize);
    this.enableEncryption = Objects.requireNonNull(enableEncryption);
    this.kmsKeyId = kmsKeyId;
  }

  public String getVolumeType() {
    return volumeType;
  }

  public int getVolumeSize() {
    return volumeSize;
  }

  public boolean isEnableEncryption() {
    return enableEncryption;
  }

  @Nullable
  public String getKmsKeyId() {
    return kmsKeyId;
  }

  BlockDeviceMapping toBlockDeviceMapping(String deviceName) {
    if (kmsKeyId != null) {
      throw new IllegalStateException("Can't have block device mapping with KMS Key ID");
    }

    EbsBlockDevice ebs = new EbsBlockDevice()
        .withVolumeType(getVolumeType())
        .withVolumeSize(getVolumeSize())
        .withEncrypted(isEnableEncryption())
        .withDeleteOnTermination(true);

    return new BlockDeviceMapping()
        .withDeviceName(deviceName)
        .withEbs(ebs);
  }

  CreateVolumeRequest toCreateVolumeRequest(String availabilityZone,
                                            TagSpecification tagSpecification) {
    return new CreateVolumeRequest()
        .withVolumeType(getVolumeType())
        .withSize(getVolumeSize())
        .withAvailabilityZone(availabilityZone)
        .withEncrypted(isEnableEncryption())
        .withKmsKeyId(getKmsKeyId())
        .withTagSpecifications(tagSpecification);
  }

  public static List<SystemDisk> parse(String systemDisksJson) {
    if (systemDisksJson == null) {
      return Collections.emptyList();
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(systemDisksJson, new TypeReference<List<SystemDisk>>(){});
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse system systemDisks Json: " + systemDisksJson, e);
    }
  }
}
