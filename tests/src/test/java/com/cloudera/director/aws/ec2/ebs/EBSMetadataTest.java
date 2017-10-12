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

import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static com.cloudera.director.aws.ec2.ebs.EBSMetadata.EBSMetadataConfigProperties.EBSMetadataConfigurationPropertyToken.CUSTOM_EBS_METADATA_PATH;
import static com.cloudera.director.aws.ec2.ebs.EBSMetadata.EbsVolumeMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import java.io.File;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit test for {@link EBSMetadata}.
 */
public class EBSMetadataTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final Map<String, String> configurationMap = ImmutableMap.<String, String>builder()
      .put(CUSTOM_EBS_METADATA_PATH.unwrap().getConfigKey(), "customebsmetadata.properties")
      .build();

  private static File configurationDirectory;

  @BeforeClass
  public static void setUpClass() {
    try {
      configurationDirectory = new File(Resources.getResource("com/cloudera/director/aws/ebs").toURI());
    } catch (Exception e) {
      fail("Unable to locate configuration directory");
    }
  }

  private EBSMetadata ebsMetadata;

  @Before
  public void setUp() {
    ebsMetadata = new EBSMetadata(
        new SimpleConfiguration(configurationMap), configurationDirectory,
        DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  @Test
  public void testWithNonOverrideBuiltIn() {
    EbsVolumeMetadata metadata = ebsMetadata.apply("st1");
    assertThat(metadata.getMinSizeGiB()).isEqualTo(500);
    assertThat(metadata.getMaxSizeGiB()).isEqualTo(16384);
  }

  @Test
  public void testWithNonOverrideCustom() {
    EbsVolumeMetadata metadata = ebsMetadata.apply("volumetype1");
    assertThat(metadata.getMinSizeGiB()).isEqualTo(20);
    assertThat(metadata.getMaxSizeGiB()).isEqualTo(300);
  }

  @Test
  public void testWithOverride() {
    EbsVolumeMetadata metadata = ebsMetadata.apply("sc1");
    assertThat(metadata.getMinSizeGiB()).isEqualTo(10);
    assertThat(metadata.getMaxSizeGiB()).isEqualTo(20);
  }

  @Test
  public void testMissing() {
    thrown.expect(NullPointerException.class);
    ebsMetadata.apply("nonexisting");
  }

  @Test
  public void testWrongFormat() {
    thrown.expect(IllegalStateException.class);
    ebsMetadata.apply("wrongformat");
  }
}
