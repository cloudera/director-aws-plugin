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

package com.cloudera.director.rds;

import java.util.Locale;


/**
 * An enum for the possible statuses of RDS instances. For a complete list, see:
 * http://docs.aws.amazon.com/AmazonRDS/latest/CommandLineReference/CLIReference-cmd-DescribeDBInstances.html
 */
public enum RDSStatus {
  AVAILABLE,
  BACKING_UP,
  CREATING,
  DELETED,
  DELETING,
  FAILED,
  INCOMPATIBLE_RESTORE,
  INCOMPATIBLE_PARAMETERS,
  MODIFYING,
  REBOOTING,
  RESETTING_MASTER_CREDENTIALS,
  STORAGE_FULL;

  /**
   * Gets the string value of this status used in RDS calls.
   *
   * @return string value
   */
  public String toRDSString() {
    return toString().toLowerCase(Locale.US).replaceAll("_", "-");
  }

  /**
   * Gets the enum with the given RDS string value.
   *
   * @param s string value
   * @return corresponding enum
   * @throws NullPointerException     if s is null
   * @throws IllegalArgumentException if s does not correspond to an enum
   */
  public static RDSStatus valueOfRDSString(String s) {
    return valueOf(RDSStatus.class, s.toUpperCase(Locale.US).replaceAll("-", "_"));
  }
}
