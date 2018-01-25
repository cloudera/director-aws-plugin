// (c) Copyright 2017 Cloudera, Inc.
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

package com.cloudera.director.aws;

import static junit.framework.TestCase.assertTrue;

import com.cloudera.director.aws.ec2.DeviceNameUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class DeviceNameUtilsTest {
  private final DeviceNameUtils deviceNameUtils = new DeviceNameUtils();

  @Test
  public void testGetEbsDeviceNames() {
    List<String> expected = Lists.newArrayList("/dev/sdf", "/dev/sdg", "/dev/sdh");
    List<String> deviceNames = deviceNameUtils.getDeviceNames("/dev/sd", 'f', expected.size(),
        Collections.<String>emptySet());
    assertTrue(deviceNames.equals(expected));
  }

  @Test
  public void testGetEbsDeviceNames_wrap() {
    // Make sure that the device characters wrap around after z.
    List<String> expected = Lists.newArrayList(
        "/dev/sdf", "/dev/sdg", "/dev/sdh", "/dev/sdi", "/dev/sdj",
        "/dev/sdk", "/dev/sdl", "/dev/sdm", "/dev/sdn", "/dev/sdo",
        "/dev/sdp", "/dev/sdq", "/dev/sdr", "/dev/sds", "/dev/sdt",
        "/dev/sdu", "/dev/sdv", "/dev/sdw", "/dev/sdx", "/dev/sdy",
        "/dev/sdz", "/dev/sdb", "/dev/sdc"
    );
    List<String> deviceNames = deviceNameUtils.getDeviceNames("/dev/sd", 'f', expected.size(),
        Collections.<String>emptySet());
    assertTrue(deviceNames.equals(expected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetEbsDeviceNamesFail_NotInRange() {
      deviceNameUtils.getDeviceNames("/dev/sd", 'a', 2, Collections.<String>emptySet());
  }

  @Test
  public void testGetEbsDeviceNames_filter() {
    Set<String> filter = Sets.newHashSet("/dev/sdg", "/dev/sdh");
    List<String> deviceNames =  deviceNameUtils.getDeviceNames("/dev/sd", 'f', 3, filter);

    List<String> expected = Lists.newArrayList("/dev/sdf", "/dev/sdi", "/dev/sdj");
    assertTrue(deviceNames.equals(expected));
  }
}
