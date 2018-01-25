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

package com.cloudera.director.aws.test;

import static org.junit.Assert.fail;

import com.cloudera.director.aws.STSAssumeNRolesSessionCredentialsProvider.RoleConfiguration;
import com.google.common.collect.ImmutableList;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Encapsulates configuration properties needed to run live tests on AWS.
 */
public class LiveTestProperties {

  private String testRegion;
  private String testSubnet;
  private String testSecurityGroup;
  private String testKmsKeyArn;

  // live test properties for delegated role access
  private List<RoleConfiguration> delegatedRoleConfigurations;
  private String externalAccountRegion;
  private String externalAccountSubnetId;
  private String externalAccountSecurityGroup;
  private String externalAccountKmsKeyArn;

  private LiveTestProperties(
      String testRegion, String testSubnet, String testSecurityGroup,
      String testKmsKeyArn, List<RoleConfiguration> roleConfigurations,
      String externalAccountRegion, String externalAccountSubnetId,
      String externalAccountSecurityGroup, String externalAccountKmsKeyArn) {
    this.testRegion = testRegion;
    this.testSubnet = testSubnet;
    this.testSecurityGroup = testSecurityGroup;
    this.testKmsKeyArn = testKmsKeyArn;
    this.delegatedRoleConfigurations = roleConfigurations;
    this.externalAccountRegion = externalAccountRegion;
    this.externalAccountSubnetId = externalAccountSubnetId;
    this.externalAccountSecurityGroup = externalAccountSecurityGroup;
    this.externalAccountKmsKeyArn = externalAccountKmsKeyArn;
  }

  private static String getProperty(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null) {
      fail("Could not find " + key + " in properties file");
    }
    return value;
  }

  public static LiveTestProperties loadLiveTestProperties() {
    String liveTestPropertyFile = System.getProperty("test.aws.live.file");

    if (liveTestPropertyFile == null) {
      fail("live test file property is not set");
    }

    Properties prop = new Properties();

    LiveTestProperties liveTestProperties = null;
    try(InputStream input = new FileInputStream(liveTestPropertyFile)) {
      prop.load(input);

      String testRegion = getProperty(prop, "region");
      String testSubnet = getProperty(prop, "subnet_id");
      String testSecurityGroup = getProperty(prop, "security_group");
      String testKmsKeyArn = getProperty(prop, "kms_key_arn");
      String externalAccountRegion = getProperty(prop, "external_account_region");
      String externalAccountSubnetId = getProperty(prop, "external_account_subnet_id");
      String externalAccountSecurityGroup = getProperty(prop, "external_account_security_group");
      String externalAccountKmsKeyArn = getProperty(prop, "external_account_kms_key");

      ImmutableList.Builder<RoleConfiguration> roleConfigurationsBuilder = ImmutableList.builder();
      for (int i = 0; i < 10; ++i) {
        String delegatedRoleArn = prop.getProperty("delegated_role_arn_" + i, null);
        if (delegatedRoleArn == null) {
          break;
        }
        String delegatedRoleSessionName = prop.getProperty(
            "delegated_role_session_name_" + i,
            "director-live-test");
        String delegatedRoleExternalId = prop.getProperty(
            "delegated_role_external_id_" + i,
            null);
        roleConfigurationsBuilder.add(
            new RoleConfiguration(delegatedRoleArn, delegatedRoleSessionName, delegatedRoleExternalId));
      }

      liveTestProperties = new LiveTestProperties(
          testRegion,
          testSubnet,
          testSecurityGroup,
          testKmsKeyArn,
          roleConfigurationsBuilder.build(),
          externalAccountRegion,
          externalAccountSubnetId,
          externalAccountSecurityGroup,
          externalAccountKmsKeyArn
      );
    } catch (IOException e) {
      e.printStackTrace();
      fail("Could not load live test properties file with path " + liveTestPropertyFile);
    }
    return liveTestProperties;
  }

  public String getTestRegion() {
    return testRegion;
  }

  public String getTestSubnet() {
    return testSubnet;
  }

  public String getTestSecurityGroup() {
    return testSecurityGroup;
  }

  public String getTestKmsKeyArn() {
    return testKmsKeyArn;
  }

  public List<RoleConfiguration> getDelegatedRoleConfigurations() {
    return delegatedRoleConfigurations;
  }

  public String getExternalAccountRegion() {
    return externalAccountRegion;
  }

  public String getExternalAccountSubnetId() {
    return externalAccountSubnetId;
  }

  public String getExternalAccountSecurityGroup() {
    return externalAccountSecurityGroup;
  }

  public String getExternalAccountKmsKeyArn() {
    return externalAccountKmsKeyArn;
  }
}
