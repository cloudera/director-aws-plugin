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

package com.cloudera.director.aws.ec2;

import static com.cloudera.director.spi.v2.util.Preconditions.checkArgument;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * Contains methods to get device names.
 */
public class DeviceNameUtils {

  /**
   * Retrieves the specified number of device names. Device character suffix will wrap
   * around to the beginning after 'z' to 'b'. Note that char 'a' is skipped because
   * it is usually reserved for root volumes.
   *
   * @param pathPrefix  the device name path prefix
   * @param startSuffix the character suffix to start with for the device name
   * @param count       the number of device names to return
   * @param filter      a set of device names to exclude
   * @return list of device names
   */
  public List<String> getDeviceNames(String pathPrefix, char startSuffix, int count, Set<String> filter) {
    checkArgument(startSuffix >= 'b' && startSuffix <= 'z', "startSuffix should be between 'b' and 'z'");
    List<String> result = Lists.newArrayListWithExpectedSize(count);
    char suffix = startSuffix;
    for (int i = 0; i < count; i++) {
      String deviceName = pathPrefix + suffix;

      if (filter.contains(deviceName)) {
        i--;
      } else {
        result.add(deviceName);
      }

      suffix++;
      // Device character suffix should wrap around to the beginning after z.
      if (suffix == 'z' + 1) suffix = 'b';
    }
    return result;
  }
}
