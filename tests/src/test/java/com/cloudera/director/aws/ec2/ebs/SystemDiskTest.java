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

import com.cloudera.director.aws.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.cloudera.director.aws.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.assertj.core.util.Maps;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemDiskTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSystemDisk_InvalidJson() {
    thrown.expect(IllegalArgumentException.class);
    SystemDisk.parse("asdf");
  }

  @Test
  public void testSystemDisk_MissingVolumeType() {
    thrown.expect(IllegalArgumentException.class);
    String config = getJsonConfig(null, "100", "false", null);
    SystemDisk.parse("[" + config + "]");
  }

  @Test
  public void testSystemDisk_MissingVolumeSize() {
    thrown.expect(IllegalArgumentException.class);
    String config = getJsonConfig("gp2", null, "false", null);
    SystemDisk.parse("[" + config + "]");
  }

  @Test
  public void testSystemDisk_MissingEncryptionFlag() {
    thrown.expect(IllegalArgumentException.class);
    String config = getJsonConfig("gp2", "100", null, null);
    SystemDisk.parse("[" + config + "]");
  }

  @Test
  public void testSystemDisk_none() {
    List<SystemDisk> systemDisk = SystemDisk.parse(null);
    assertThat(systemDisk).isEmpty();
  }

  @Test
  public void testSystemDisk() {
    String type = "gp2";
    String volumeSize = "100";
    String enableEncryption = "false";

    String config = getJsonConfig(type, volumeSize, enableEncryption, null);
    config = "[" + config + "]";

    SystemDisk systemDisk = Iterables.getOnlyElement(SystemDisk.parse(config));
    assertThat(systemDisk.getVolumeType()).isEqualTo(type);
    assertThat(systemDisk.getVolumeSize()).isEqualTo(Integer.parseInt(volumeSize));
    assertThat(systemDisk.isEnableEncryption()).isEqualTo(Boolean.parseBoolean(enableEncryption));
    assertThat(systemDisk.getKmsKeyId()).isEqualTo(null);
  }

  @Test
  public void testSystemDisk_WithEncryption() {
    String type = "gp2";
    String volumeSize = "100";
    String enableEncryption = "true";
    String kmsKeyId = "arn:aws:kms:xxxx";

    String config = getJsonConfig(type, volumeSize, enableEncryption, kmsKeyId);
    config = "[" + config + "]";

    SystemDisk systemDisk = Iterables.getOnlyElement(SystemDisk.parse(config));
    assertThat(systemDisk.getVolumeType()).isEqualTo(type);
    assertThat(systemDisk.getVolumeSize()).isEqualTo(Integer.parseInt(volumeSize));
    assertThat(systemDisk.isEnableEncryption()).isEqualTo(Boolean.parseBoolean(enableEncryption));
    assertThat(systemDisk.getKmsKeyId()).isEqualTo(kmsKeyId);
  }

  @Test
  public void testSystemDisk_multiple() {
    String type[] = { "gp2", "gp2", "st1" };
    String volumeSize[] = { "500", "600", "700" };
    String enableEncryption[] = { "true", "false", "true" };

    StringBuilder systemDiskConfig = new StringBuilder();
    systemDiskConfig.append("[ ");
    for (int i = 0; i < type.length; i++) {
      String config = getJsonConfig(type[i], volumeSize[i], enableEncryption[i], null);
      systemDiskConfig.append(config);
      if (i != type.length - 1) {
        systemDiskConfig.append(", ");
      }
    }
    systemDiskConfig.append(" ]");

    List<SystemDisk> systemDisks = SystemDisk.parse(systemDiskConfig.toString());
    for (int i = 0; i < type.length; i++) {
      SystemDisk systemDisk = systemDisks.get(i);
      assertThat(systemDisk.getVolumeType()).isEqualTo(type[i]);
      assertThat(systemDisk.getVolumeSize()).isEqualTo(Integer.parseInt(volumeSize[i]));
      assertThat(systemDisk.isEnableEncryption()).isEqualTo(Boolean.parseBoolean(enableEncryption[i]));
      assertThat(systemDisk.getKmsKeyId()).isEqualTo(null);
    }
  }

  private static String getJsonConfig(String volumeType, String volumeSize,
                                      String enableEncryption, String kmsKeyId) {
    Map<String, String> config = Maps.newHashMap();
    if (volumeType != null) config.put("volumeType", volumeType);
    if (volumeSize != null) config.put("volumeSize", volumeSize);
    if (enableEncryption != null) config.put("enableEncryption", enableEncryption);
    if (kmsKeyId != null) config.put("kmsKeyId", kmsKeyId);

    try {
      return new ObjectMapper().writeValueAsString(config);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to write JSON string", e);
    }
  }
}
