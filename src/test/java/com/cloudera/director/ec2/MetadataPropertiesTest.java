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

package com.cloudera.director.ec2;

import static org.assertj.core.api.Assertions.assertThat;

import com.cloudera.director.aws.AWSLauncher;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

/**
 * Check plugin metadata invariants
 */
public class MetadataPropertiesTest {

  private static final String DEFAULT_EPHEMERAL_DEVICE_MAPPINGS =
      "/com/cloudera/director/ec2/ephemeraldevicemappings.properties";

  private static final VirtualizationMappings VIRTUALIZATION_MAPPINGS =
      new VirtualizationMappings(new SimpleConfiguration(new HashMap<String, String>()),
          AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

  @Test
  public void testEphemeralDeviceMappingsAreInSyncWithVirtualizationMappings() throws IOException {
    Properties ephemeralDeviceMappings = new Properties();
    ephemeralDeviceMappings.load(
        ClassLoader.class.getResourceAsStream(DEFAULT_EPHEMERAL_DEVICE_MAPPINGS));

    Set<String> instanceTypes = Sets.newHashSet();
    instanceTypes.addAll(VIRTUALIZATION_MAPPINGS.apply("hvm"));
    instanceTypes.addAll(VIRTUALIZATION_MAPPINGS.apply("paravirtual"));

    assertThat(ephemeralDeviceMappings.keySet()).isEqualTo(instanceTypes);
  }
}
