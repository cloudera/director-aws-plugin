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

package com.cloudera.director.aws;

/**
 * Constants for important properties and sections in the configuration file
 *
 * @see <a href="https://github.com/typesafehub/config" />
 */
public final class Configurations {

  private Configurations() {
  }

  /**
   * The configuration file name.
   */
  public static final String CONFIGURATION_FILE_NAME = "aws-plugin.conf";

  /**
   * The HOCON path prefix for ephemeral device mapping configuration.
   */
  public static final String EPHEMERAL_DEVICE_MAPPINGS_SECTION = "ephemeralDeviceMappings";

  /**
   * The HOCON path prefix for virtualization mapping configuration.
   */
  public static final String VIRTUALIZATION_MAPPINGS_SECTION = "virtualizationMappings";

  /**
   * The HOCON path prefix for RDS endpoints configuration.
   */
  public static final String RDS_ENDPOINTS_SECTION = "rdsEndpoints";

  /**
   * The HOCON path prefix for HTTP proxy configuration.
   */
  public static final String HTTP_PROXY_SECTION = "httpProxy";

  /**
   * The HOCON path prefix for AWS client configuration.
   */
  public static final String AWS_CLIENT_SECTION = "awsClient";
}
