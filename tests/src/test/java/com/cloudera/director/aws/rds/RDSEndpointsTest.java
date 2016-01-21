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

import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.cloudera.director.aws.rds.RDSEndpoints.RDSEndpointsConfigProperties.RDSEndpointsConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import java.io.File;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for {@link RDSEndpoints}.
 */
public class RDSEndpointsTest {

  private final Map<String, String> configurationMap = ImmutableMap.<String, String>builder()
      .put(RDSEndpointsConfigurationPropertyToken.CUSTOM_ENDPOINTS_PATH.unwrap().getConfigKey(),
           "customendpoints.properties")
      .build();

  private static File configurationDirectory;

  @BeforeClass
  public static void setUpClass() {
    try {
      configurationDirectory = new File(Resources.getResource("com/cloudera/director/aws/rds").toURI());
    } catch (Exception e) {
      fail("Unable to locate configuration directory");
    }
  }

  private RDSEndpoints rdsEndpoints;

  @Before
  public void setUp() {
    rdsEndpoints = new RDSEndpoints(
        new SimpleConfiguration(configurationMap), configurationDirectory,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  @Test
  public void testWithNonOverrideBuiltIn() {
    String endpoint = rdsEndpoints.apply("us-east-1");
    assertThat(endpoint).isEqualTo("https://rds.us-east-1.amazonaws.com");
  }

  @Test
  public void testWithNonOverrideCustom() {
    String endpoint = rdsEndpoints.apply("regionx");
    assertThat(endpoint).isEqualTo("https://rds.regionx.amazonaws.com");
  }

  @Test
  public void testWithOverride() {
    String endpoint = rdsEndpoints.apply("us-west-2");
    assertThat(endpoint).isEqualTo("https://rds-override.us-west-2.amazonaws.com");
  }

  @Test
  public void testMissing() {
    assertThat(rdsEndpoints.apply("region0")).isNull();
  }

  @Test
  public void testTestInstance() {
    Map<String, String> m = Maps.newHashMap();
    m.put("regiona", "urla");
    m.put("regionb", "urlb");

    RDSEndpoints rdsEndpoints = RDSEndpoints.getTestInstance(m, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    String endpoint = rdsEndpoints.apply("regiona");
    assertThat(endpoint).isEqualTo("urla");
    endpoint = rdsEndpoints.apply("regionb");
    assertThat(endpoint).isEqualTo("urlb");
    assertThat(rdsEndpoints.apply("regionc")).isNull();
  }
}
