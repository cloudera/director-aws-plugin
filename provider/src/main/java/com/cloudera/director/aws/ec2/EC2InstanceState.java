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

import com.amazonaws.services.ec2.model.InstanceStateName;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.util.AbstractInstanceState;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 * EC2 instance state implementation.
 */
public class EC2InstanceState extends AbstractInstanceState<InstanceStateName> {

  /**
   * The map from EC2 instance state names to Director instance state.
   */
  private static final Map<InstanceStateName, EC2InstanceState> INSTANCE_STATE_MAP;

  /**
   * Unknown instance state.
   */
  private static final EC2InstanceState UNKNOWN = new EC2InstanceState(InstanceStatus.UNKNOWN, null);

  static {
    Map<InstanceStateName, EC2InstanceState> map = Maps.newEnumMap(InstanceStateName.class);
    addInstanceState(map, InstanceStateName.Pending, InstanceStatus.PENDING);
    addInstanceState(map, InstanceStateName.Running, InstanceStatus.RUNNING);
    addInstanceState(map, InstanceStateName.ShuttingDown, InstanceStatus.DELETING);
    addInstanceState(map, InstanceStateName.Terminated, InstanceStatus.DELETED);
    addInstanceState(map, InstanceStateName.Stopping, InstanceStatus.STOPPING);
    addInstanceState(map, InstanceStateName.Stopped, InstanceStatus.STOPPED);
    INSTANCE_STATE_MAP = Collections.unmodifiableMap(map);
  }

  /**
   * Returns the Director instance state for the specified EC2 instance state name.
   *
   * @param instanceStateName the EC2 instance state name
   * @return the corresponding Director instance state
   */
  public static EC2InstanceState fromInstanceStateName(InstanceStateName instanceStateName) {
    return (instanceStateName == null) ? UNKNOWN : INSTANCE_STATE_MAP.get(instanceStateName);
  }

  /**
   * Adds an entry in the specified map associating the specified EC2 instance state name with a Director instance state
   * with the corresponding instance status.
   *
   * @param map               the map from EC2 instance state names to Director instance states
   * @param instanceStateName the EC2 instance state name
   * @param instanceStatus    the corresponding instance status
   */
  private static void addInstanceState(Map<InstanceStateName, EC2InstanceState> map,
      InstanceStateName instanceStateName, InstanceStatus instanceStatus) {
    map.put(instanceStateName, new EC2InstanceState(instanceStatus, instanceStateName));
  }

  /**
   * Creates an EC2 instance state with the specified parameters.
   *
   * @param instanceStatus       the instance status
   * @param instanceStateDetails the provider-specific instance state details
   */
  private EC2InstanceState(InstanceStatus instanceStatus, InstanceStateName instanceStateDetails) {
    super(instanceStatus, instanceStateDetails);
  }

  @Override
  public String toString() {
    InstanceStateName details = unwrap();
    return getInstanceStatus().toString() + " (EC2: " + (details != null ? details.toString() : "unknown") + ")";
  }
}
