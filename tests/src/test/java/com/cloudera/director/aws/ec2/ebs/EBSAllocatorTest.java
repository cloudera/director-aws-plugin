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

import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;

public class EBSAllocatorTest {

  private EBSAllocator ebsAllocator;

  @Before
  public void setUp() {
    ebsAllocator = new EBSAllocator(mock(AmazonEC2Client.class), new AWSTimeouts(null),
        new EC2TagHelper(new CustomTagMappings(null)));
  }

  @Test
  public void testGetEbsDeviceNames() {
    List<String> expected = Lists.newArrayList("/dev/sdf", "/dev/sdg", "/dev/sdh");
    int count = expected.size();

    List<String> deviceNames = ebsAllocator.getEbsDeviceNames(count);
    assertTrue(deviceNames.equals(expected));
  }

  @Test
  public void testGetEbsDeviceNames_wrap() {
    // Make sure that the device characters wrap around after z
    // and skip 'a' character (a is reserved for root).
    List<String> expected = Lists.newArrayList(
        "/dev/sdf", "/dev/sdg", "/dev/sdh", "/dev/sdi", "/dev/sdj",
        "/dev/sdk", "/dev/sdl", "/dev/sdm", "/dev/sdn", "/dev/sdo",
        "/dev/sdp", "/dev/sdq", "/dev/sdr", "/dev/sds", "/dev/sdt",
        "/dev/sdu", "/dev/sdv", "/dev/sdw", "/dev/sdx", "/dev/sdy",
        "/dev/sdz", "/dev/sdb", "/dev/sdc"
    );
    int count = expected.size();

    List<String> deviceNames = ebsAllocator.getEbsDeviceNames(count);
    assertTrue(deviceNames.equals(expected));
  }
}
