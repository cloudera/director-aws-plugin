package com.cloudera.director.aws;

import static junit.framework.TestCase.assertTrue;
import com.cloudera.director.aws.ec2.DeviceNameUtils;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

public class DeviceNameUtilsTest {
  private final DeviceNameUtils deviceNameUtils = new DeviceNameUtils();

  @Test
  public void testGetEbsDeviceNames() {
    List<String> expected = Lists.newArrayList("/dev/sdf", "/dev/sdg", "/dev/sdh");
    List<String> deviceNames = deviceNameUtils.getDeviceNames("/dev/sd", 'f', expected.size());
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
    List<String> deviceNames = deviceNameUtils.getDeviceNames("/dev/sd", 'f', expected.size());
    assertTrue(deviceNames.equals(expected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetEbsDeviceNamesFail_NotInRange() {
      deviceNameUtils.getDeviceNames("/dev/sd", 'a', 2);
  }
}
