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

import static java.util.Objects.requireNonNull;

import com.cloudera.director.aws.STSAssumeNRolesSessionCredentialsProvider.RoleConfiguration;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * STS role configuration holder.
 */
public class STSRoles {
  public static final STSRoles DEFAULT = new STSRoles(Collections.<Config>emptyList());

  @VisibleForTesting
  static final String ROLE_ARN = "roleArn";
  @VisibleForTesting
  static final String ROLE_SESSION_NAME = "roleSessionName";
  @VisibleForTesting
  static final String ROLE_EXTERNAL_ID = "roleExternalId";

  private final List<RoleConfiguration> roleConfigurations;

  /**
   * Creates an instance of STSRoles with the provided config.
   *
   * @param roleConfigurations the role configurations
   */
  public STSRoles(@Nonnull List<? extends Config> roleConfigurations) {
    requireNonNull(roleConfigurations, "roleConfigurations is null");
    ImmutableList.Builder<RoleConfiguration> builder = ImmutableList.builder();

    for (Config roleConfiguration : roleConfigurations) {
      String roleArn = roleConfiguration.getString(ROLE_ARN);
      String roleSessionName = roleConfiguration.hasPath(ROLE_SESSION_NAME)
          ? roleConfiguration.getString(ROLE_SESSION_NAME)
          : parseRoleName(roleArn);
      String roleExternalId = roleConfiguration.hasPath(ROLE_EXTERNAL_ID)
          ? roleConfiguration.getString(ROLE_EXTERNAL_ID)
          : null;
      builder.add(new RoleConfiguration(roleArn, roleSessionName, roleExternalId));
    }

    this.roleConfigurations = builder.build();
  }

  /**
   * Gets the role configurations.
   */
  public List<RoleConfiguration> getRoleConfigurations() {
    return roleConfigurations;
  }

  /**
   * Parses role ARN for role name.
   *
   * @param roleArn the role ARN
   * @return the role name
   */
  public static String parseRoleName(String roleArn) {
    // Role ARN format:
    // roleArn:partition:service:region:account-id:resourcetype/resource
    // See, https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html#Identifiers_ARNs
    String[] roleArnComponents = roleArn.split(":");
    String[] resourceComponents;
    if (roleArnComponents.length == 6) {
      resourceComponents = roleArnComponents[5].split("/");
      if (resourceComponents.length == 2) {
        return resourceComponents[1];
      }
    }

    throw new IllegalArgumentException("Malformed role ARN: " + roleArn);
  }

  /**
   * Generates timestamped session name.
   * <p>
   * {@note For best tracking purpose on the owner side, it might be better
   * not to call this method, but provide an entity which tries to assume
   * role}.
   * <p>
   * {@note The role session name uniquely identifies a session when the same
   * role is assumed by different principals or for different reasons. In
   * cross-account scenarios, the role session name is visible to, and can be
   * logged by the account that owns the role. The role session name is also
   * used in the ARN of the assumed role principal. This means that subsequent
   * cross-account API requests using the temporary security credentials will
   * expose the role session name to the external account in their CloudTrail
   * logs.
   * <p>
   * The format for the session name, as described by its regex pattern, is a
   * string of characters consisting of upper- and lower-case alphanumeric
   * characters with no spaces. You can also include any of the following
   * characters: =,.@-
   * <p>
   * Its minimum length is 2, and maximum length is 64. }
   *
   * @param roleName role name
   */
  public static String getTimestampedSessionName(String roleName) {
    DateFormat dateFormat = new SimpleDateFormat("yyMMddHHmmssSSSz");
    String currentTime = dateFormat.format(new Date());

    final int maxLen = 64;
    String roleSessionName = String.format("%s-%s", currentTime, roleName);
    if (roleSessionName.length() > maxLen) {
      roleSessionName = roleSessionName.substring(0, maxLen);
    }

    return roleSessionName;
  }

}
