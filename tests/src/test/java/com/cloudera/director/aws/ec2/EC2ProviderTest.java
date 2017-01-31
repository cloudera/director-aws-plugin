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

package com.cloudera.director.aws.ec2;

import static org.assertj.core.api.Assertions.assertThat;

import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.google.common.collect.ImmutableList;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class EC2ProviderTest {

  private BlockDeviceMapping ebs1, ebs1x, ebs2;
  private BlockDeviceMapping eph2;

  @Before
  public void setUp() {
    ebs1 = new BlockDeviceMapping()
        .withDeviceName("/dev/sda1")
        .withEbs(new EbsBlockDevice()
                 .withVolumeSize(10)
                 .withVolumeType("gp2")
                 .withSnapshotId("snap-1"));
    ebs1x = new BlockDeviceMapping()
        .withDeviceName("/dev/sda")
        .withEbs(new EbsBlockDevice()
                 .withVolumeSize(10)
                 .withVolumeType("gp2")
                 .withSnapshotId("snap-1"));
    ebs2 = new BlockDeviceMapping()
        .withDeviceName("/dev/sdb1")
        .withEbs(new EbsBlockDevice()
                 .withVolumeSize(20)
                 .withVolumeType("gp2")
                 .withSnapshotId("snap-2"));

    eph2 = new BlockDeviceMapping()
        .withDeviceName("/dev/sdb")
        .withVirtualName("ephemeral0");
  }

  @Test
  public void testExactMatch() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(ebs2, ebs1, eph2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isEqualTo(ebs1);
  }

  @Test
  public void testBestMatch() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(ebs2, ebs1x, eph2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isEqualTo(ebs1x);
  }

  @Test
  public void testFirstEbs() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(eph2, ebs2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isEqualTo(ebs2);
  }

  @Test
  public void testNoRootDevice() {
    List<BlockDeviceMapping> mappings = ImmutableList.of(eph2);
    assertThat(EC2Provider.selectRootDevice(mappings, "/dev/sda1")).isNull();
  }
}
