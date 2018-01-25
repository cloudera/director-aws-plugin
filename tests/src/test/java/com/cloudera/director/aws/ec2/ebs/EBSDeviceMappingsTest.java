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

import static com.cloudera.director.spi.v2.provider.Launcher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static org.junit.Assert.assertTrue;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.cloudera.director.aws.shaded.com.google.common.base.Optional;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.BlockDeviceMapping;

import java.util.Collections;
import java.util.List;

public class EBSDeviceMappingsTest {

  private EBSDeviceMappings ebsDeviceMappings;

  @Before
  public void setUp() {
    ebsDeviceMappings = new EBSDeviceMappings(new SimpleConfiguration(), DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
  }

  @Test
  public void testGetBlockDeviceMappings() {
    int count = 3;
    String volumeType = "gp2";
    int volumeSizeGib = 20;
    Optional<Integer> iops = Optional.absent();
    boolean encrypted = false;

    List<BlockDeviceMapping> deviceMappings = ebsDeviceMappings.getBlockDeviceMappings(count, volumeType, volumeSizeGib,
        iops, false, Collections.<String>emptySet());

    String firstDeviceName = EBSDeviceMappings.DEFAULT_EBS_DEVICE_NAME_PREFIX +
        EBSDeviceMappings.DEFAULT_EBS_DEVICE_NAME_START_CHAR;

    assertTrue(deviceMappings.size() == count);
    assertTrue(deviceMappings.get(0).getDeviceName().equals(firstDeviceName));

    for (BlockDeviceMapping mapping : deviceMappings) {
      EbsBlockDevice ebsBlockDevice = mapping.getEbs();
      assertTrue(ebsBlockDevice.getVolumeType().equals(volumeType));
      assertTrue(ebsBlockDevice.getVolumeSize().equals(volumeSizeGib));
      assertTrue(ebsBlockDevice.getEncrypted() == encrypted);
    }
  }
}
