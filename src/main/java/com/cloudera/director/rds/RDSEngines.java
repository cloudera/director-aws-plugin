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

import com.cloudera.director.spi.v1.database.DatabaseType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Provides information about the supported RDS database engines.
 */
public class RDSEngines {

  /**
   * Map from supported database types to RDS-specific engine names.
   */
  protected static final Map<DatabaseType, String> ENGINES =
      ImmutableMap.of(DatabaseType.MYSQL, "MYSQL");

  /**
   * Returns the engine for the specified database type.
   *
   * @param databaseType the database type
   * @return the engine for the specified database type
   */
  protected static String getEngine(DatabaseType databaseType) {
    if (!ENGINES.containsKey(databaseType)) {
      throw new IllegalArgumentException("Unsupported database type " + databaseType);
    }
    return ENGINES.get(databaseType);
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private RDSEngines() {
  }
}
