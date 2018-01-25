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
 * Constants for important properties and sections in the configuration file.
 *
 * @see <a href="https://github.com/typesafehub/config">Typesafe Config</a>
 */
public final class Configurations {

  private Configurations() {
  }

  /**
   * The configuration file name.
   */
  public static final String CONFIGURATION_FILE_NAME = "aws-plugin.conf";

  /**
   * The configuration file name for network rules.
   */
  public static final String NETWORK_RULES_FILE_NAME = "network-rules.conf";

  /**
   * The HOCON path prefix for ephemeral device mapping configuration.
   */
  public static final String EPHEMERAL_DEVICE_MAPPINGS_SECTION = "ephemeralDeviceMappings";

  /**
   * The HOCON path prefix for EBS device mapping configuration.
   */
  public static final String EBS_DEVICE_MAPPINGS_SECTION = "ebsDeviceMappings";

  /**
   * The HOCON path prefix for virtualization mapping configuration.
   */
  public static final String VIRTUALIZATION_MAPPINGS_SECTION = "virtualizationMappings";

  /**
   * The HOCON path prefix for RDS endpoints configuration.
   */
  public static final String RDS_ENDPOINTS_SECTION = "rdsEndpoints";

  /**
   * The HOCON path prefix for RDS encryption instance classes configuration.
   */
  public static final String RDS_ENCRYPTION_INSTANCE_CLASSES_SECTION =
      "rdsEncryptionInstanceClasses";

  /**
   * The HOCON path prefix for EBS metadata configuration.
   */
  public static final String EBS_METADATA_SECTION = "ebsMetadata";

  /**
   * The HOCON path prefix for HTTP proxy configuration.
   */
  public static final String HTTP_PROXY_SECTION = "httpProxy";

  /**
   * The HOCON path prefix for AWS client configuration.
   */
  public static final String AWS_CLIENT_SECTION = "awsClient";

  /**
   * The HOCON path prefix for AWS filter configuration.
   */
  public static final String AWS_FILTERS_SECTION = "awsFilters";

  /**
   * The HOCON path prefix for AWS timeouts.
   */
  public static final String AWS_TIMEOUTS_SECTION = "awsTimeouts";

  /**
   * The HOCON path prefix for custom tag names.
   */
  public static final String CUSTOM_TAG_MAPPINGS_SECTION = "customTagMappings";

  /**
   * The HOCON path prefix for STS role configurations.
   */
  public static final String STS_ROLES_SECTION = "stsRoles";

  /**
   * The HOCON path prefix for using tag on create.
   */
  public static final String USE_TAG_ON_CREATE = "useTagOnCreate";
}
