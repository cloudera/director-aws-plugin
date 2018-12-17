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
import static com.cloudera.director.aws.ec2.VirtualizationMappings.VirtualizationMappingsConfigProperties.VirtualizationMappingsConfigurationPropertyToken.CUSTOM_MAPPINGS_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;
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
 * Unit test for {@link VirtualizationMappings}.
 */
public class VirtualizationMappingsTest {

  private final Map<String, String> configurationMap = ImmutableMap.<String, String>builder()
      .put(CUSTOM_MAPPINGS_PATH.unwrap().getConfigKey(), "customvirtualizationmappings.properties")
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

  private VirtualizationMappings virtualizationMappings;

  @Before
  public void setUp() {
    virtualizationMappings = new VirtualizationMappings(
        new SimpleConfiguration(configurationMap), configurationDirectory,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  // When new HVM instance types are added to virtualizationmappings.properties,
  // update this constant.

  private static final int NUM_HVM_INSTANCE_TYPES = 111;

  @Test
  public void testWithNonOverrideBuiltIn() {
    List<String> instanceTypes = virtualizationMappings.apply("hvm");
    assertThat(instanceTypes).contains("c3.large").hasSize(NUM_HVM_INSTANCE_TYPES);
  }

  @Test
  public void testWithNonOverrideCustom() {
    List<String> instanceTypes = virtualizationMappings.apply("virtualization3");
    assertThat(instanceTypes).containsExactly("type3");
  }

  @Test
  public void testWithOverride() {
    List<String> instanceTypes = virtualizationMappings.apply("virtualization1");
    assertThat(instanceTypes).containsExactly("type1x", "type1y", "type1z");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissing() {
    virtualizationMappings.apply("virtualizationx");
  }

  @Test
  public void testTestInstance() {
    Map<String, List<String>> m = Maps.newHashMap();
    m.put("virtualization1", ImmutableList.of("type1_1", "type1_2"));
    m.put("virtualization2", ImmutableList.of("type2_1"));

    VirtualizationMappings virtualizationMappings =
        VirtualizationMappings.getTestInstance(m, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    List<String> instanceTypes = virtualizationMappings.apply("virtualization1");
    assertThat(instanceTypes).containsExactly("type1_1", "type1_2");
    instanceTypes = virtualizationMappings.apply("virtualization2");
    assertThat(instanceTypes).containsExactly("type2_1");
    try {
      virtualizationMappings.apply("virtualizationx");
      fail("Found missing virtualization in test instance");
    } catch (IllegalArgumentException e) {
      /* good! */
    }
  }
}
