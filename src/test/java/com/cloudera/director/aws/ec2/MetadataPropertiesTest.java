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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

/**
 * Check plugin metadata invariants
 */
public class MetadataPropertiesTest {

  private static final String DEFAULT_EPHEMERAL_DEVICE_MAPPINGS =
      "/com/cloudera/director/aws/ec2/ephemeraldevicemappings.properties";

  private static final String DEFAULT_VIRTUALIZATION_MAPPINGS =
      "/com/cloudera/director/aws/ec2/virtualizationmappings.properties";

  @Test
  public void testEphemeralDeviceMappingsAreInSyncWithVirtualizationMappings() throws IOException {
    Properties ephemeralDeviceMappings = new Properties();
    ephemeralDeviceMappings.load(
        ClassLoader.class.getResourceAsStream(DEFAULT_EPHEMERAL_DEVICE_MAPPINGS));

    Properties virtualizationMappings = new Properties();
    virtualizationMappings.load(
        ClassLoader.class.getResourceAsStream(DEFAULT_VIRTUALIZATION_MAPPINGS));

    Set<String> instanceTypes = Sets.newHashSet();
    for (Object vmValue : virtualizationMappings.values()) {
      instanceTypes.addAll(Splitter.on(",").splitToList(vmValue.toString()));
    }

    assertThat(ephemeralDeviceMappings.keySet()).isEqualTo(instanceTypes);
  }
}
