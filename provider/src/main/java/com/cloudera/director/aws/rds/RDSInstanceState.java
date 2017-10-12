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

package com.cloudera.director.aws.rds;

import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.util.AbstractInstanceState;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 * RDS instance state implementation.
 */
public class RDSInstanceState extends AbstractInstanceState<RDSStatus> {

  /**
   * The map from RDS statuses to Director instance state.
   */
  private static final Map<RDSStatus, RDSInstanceState> INSTANCE_STATE_MAP;

  /**
   * Unknown instance state.
   */
  private static final RDSInstanceState UNKNOWN = new RDSInstanceState(InstanceStatus.UNKNOWN, null);

  static {
    Map<RDSStatus, RDSInstanceState> map = Maps.newEnumMap(RDSStatus.class);
    addInstanceState(map, RDSStatus.AVAILABLE, InstanceStatus.RUNNING);
    addInstanceState(map, RDSStatus.BACKING_UP, InstanceStatus.PENDING);
    addInstanceState(map, RDSStatus.CREATING, InstanceStatus.PENDING);
    addInstanceState(map, RDSStatus.MODIFYING, InstanceStatus.PENDING);
    addInstanceState(map, RDSStatus.REBOOTING, InstanceStatus.PENDING);
    addInstanceState(map, RDSStatus.RESETTING_MASTER_CREDENTIALS, InstanceStatus.PENDING);
    addInstanceState(map, RDSStatus.DELETING, InstanceStatus.DELETING);
    addInstanceState(map, RDSStatus.DELETED, InstanceStatus.DELETED);
    addInstanceState(map, RDSStatus.FAILED, InstanceStatus.FAILED);
    addInstanceState(map, RDSStatus.INCOMPATIBLE_RESTORE, InstanceStatus.FAILED);
    addInstanceState(map, RDSStatus.INCOMPATIBLE_PARAMETERS, InstanceStatus.FAILED);
    addInstanceState(map, RDSStatus.STORAGE_FULL, InstanceStatus.FAILED);
    INSTANCE_STATE_MAP = Collections.unmodifiableMap(map);
  }

  /**
   * Returns the Director instance state for the specified RDS status.
   *
   * @param rdsStatus the RDS status
   * @return the corresponding Director instance state
   */
  public static RDSInstanceState fromRdsStatus(RDSStatus rdsStatus) {
    return (rdsStatus == null) ? UNKNOWN : INSTANCE_STATE_MAP.get(rdsStatus);
  }

  /**
   * Adds an entry in the specified map associating the specified RDS status with a Director instance state
   * with the corresponding instance status.
   *
   * @param map               the map from RDS instance statuses to Director instance states
   * @param instanceStateName the RDS status
   * @param instanceStatus    the corresponding instance status
   */
  private static void addInstanceState(Map<RDSStatus, RDSInstanceState> map,
      RDSStatus instanceStateName, InstanceStatus instanceStatus) {
    map.put(instanceStateName, new RDSInstanceState(instanceStatus, instanceStateName));
  }

  /**
   * Creates an RDS instance state with the specified parameters.
   *
   * @param instanceStatus       the instance status
   * @param instanceStateDetails the provider-specific instance state details
   */
  public RDSInstanceState(InstanceStatus instanceStatus, RDSStatus instanceStateDetails) {
    super(instanceStatus, instanceStateDetails);
  }
}
