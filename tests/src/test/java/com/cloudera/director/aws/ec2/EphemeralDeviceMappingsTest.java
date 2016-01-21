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

import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static com.cloudera.director.aws.ec2.EphemeralDeviceMappings.EphemeralDeviceMappingsConfigProperties.EphemeralDeviceMappingsConfigurationPropertyToken.CUSTOM_MAPPINGS_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for {@link EphemeralDeviceMappings}.
 */
public class EphemeralDeviceMappingsTest {

  private final Map<String, String> configurationMap = ImmutableMap.<String, String>builder()
      .put(CUSTOM_MAPPINGS_PATH.unwrap().getConfigKey(), "customephemeraldevicemappings.properties")
      .build();

  private static File configurationDirectory;

  @BeforeClass
  public static void setUpClass() {
    try {
      configurationDirectory = new File(Resources.getResource("com/cloudera/director/aws/ec2").toURI());
    } catch (Exception e) {
      fail("Unable to locate configuration directory");
    }
  }

  private EphemeralDeviceMappings ephemeralDeviceMappings;

  @Before
  public void setUp() {
    ephemeralDeviceMappings = new EphemeralDeviceMappings(
        new SimpleConfiguration(configurationMap), configurationDirectory,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  @Test
  public void testGetLinuxDeviceNames() {
    List<String> deviceNames = ephemeralDeviceMappings.getLinuxDeviceNames("/dev/sd", 'a', 3);
    assertThat(deviceNames).containsExactly("/dev/sda", "/dev/sdb", "/dev/sdc");
  }

  @Test
  public void testWithZero() {
    assertThat(ephemeralDeviceMappings.apply("instancetype0")).isEmpty();
  }

  @Test
  public void testWithNonOverrideBuiltIn() {
    List<BlockDeviceMapping> deviceMappings = ephemeralDeviceMappings.apply("c3.large");

    assertThat(deviceMappings).hasSize(2);
    assertThat(deviceMappings.get(0).getDeviceName()).isEqualTo("/dev/sdb");
    assertThat(deviceMappings.get(0).getVirtualName()).isEqualTo("ephemeral0");

    assertThat(deviceMappings.get(1).getDeviceName()).isEqualTo("/dev/sdc");
    assertThat(deviceMappings.get(1).getVirtualName()).isEqualTo("ephemeral1");
  }

  @Test
  public void testWithNonOverrideCustom() {
    List<BlockDeviceMapping> deviceMappings = ephemeralDeviceMappings.apply("instancetype3");

    assertThat(deviceMappings).hasSize(3);
    assertThat(deviceMappings.get(0).getDeviceName()).isEqualTo("/dev/sdb");
    assertThat(deviceMappings.get(0).getVirtualName()).isEqualTo("ephemeral0");

    assertThat(deviceMappings.get(2).getDeviceName()).isEqualTo("/dev/sdd");
    assertThat(deviceMappings.get(2).getVirtualName()).isEqualTo("ephemeral2");
  }

  @Test
  public void testWithOverride() {
    List<BlockDeviceMapping> deviceMappings = ephemeralDeviceMappings.apply("instancetype1");

    assertThat(deviceMappings).hasSize(4);
    assertThat(deviceMappings.get(0).getDeviceName()).isEqualTo("/dev/sdb");
    assertThat(deviceMappings.get(0).getVirtualName()).isEqualTo("ephemeral0");

    assertThat(deviceMappings.get(3).getDeviceName()).isEqualTo("/dev/sde");
    assertThat(deviceMappings.get(3).getVirtualName()).isEqualTo("ephemeral3");
  }

  @Test
  public void testWithLots() {
    List<BlockDeviceMapping> deviceMappings = ephemeralDeviceMappings.apply("instancetype24");

    assertThat(deviceMappings).hasSize(24);
    assertThat(deviceMappings.get(0).getDeviceName()).isEqualTo("/dev/sdb");
    assertThat(deviceMappings.get(23).getDeviceName()).isEqualTo("/dev/sdy");
    assertThat(deviceMappings.get(23).getVirtualName()).isEqualTo("ephemeral23");
  }

  @Test
  public void testTestInstance() {
    Map<String, Integer> counts = Maps.newHashMap();
    counts.put("instancetype1", 11);
    counts.put("instancetype2", 12);

    EphemeralDeviceMappings ephemeralDeviceMappings =
        EphemeralDeviceMappings.getTestInstance(counts, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    List<BlockDeviceMapping> deviceMappings = ephemeralDeviceMappings.apply("instancetype1");
    assertThat(deviceMappings).hasSize(11);
    deviceMappings = ephemeralDeviceMappings.apply("instancetype2");
    assertThat(deviceMappings).hasSize(12);
    deviceMappings = ephemeralDeviceMappings.apply("instancetype3");
    assertThat(deviceMappings).hasSize(0);
  }
}
