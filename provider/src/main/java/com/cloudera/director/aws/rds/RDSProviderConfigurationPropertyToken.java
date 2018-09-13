// (c) Copyright 2018 Cloudera, Inc.
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

import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;

/**
 * RDS configuration properties.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/rds/model/CreateDBInstanceRequest.html">CreateDBInstanceRequest</a>
 */
// Fully qualifying class name due to compiler bug
public enum RDSProviderConfigurationPropertyToken
    implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

  /**
   * Whether to associate a public IP address with instances. Default is <code>false</code>,
   * which differs from the RDS default.
   *
   * @see <a href="http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/vpc-ip-addressing.html">IP Addressing in your VPC</a>
   */
  ASSOCIATE_PUBLIC_IP_ADDRESSES(new SimpleConfigurationPropertyBuilder()
      .configKey("rdsAssociatePublicIpAddresses")
      .name("Associate public IP addresses")
      .widget(ConfigurationProperty.Widget.CHECKBOX)
      .defaultValue("false")
      .defaultDescription("Whether to associate a public IP address with instances or not. " +
          "If this is false, instances are expected to be able to access the internet using a NAT instance. " +
          "Currently the only way to get optimal S3 data transfer performance is to assign " +
          "public IP addresses to instances and not use NAT instances (public subnet setup).")
      .build()),

  /**
   * RDS region. Each region is a separate geographic area. Each region has multiple,
   * isolated locations known as Availability Zones. Default is {@code null}, so we can fall back
   * to the EC2 region.
   *
   * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html">Regions and Availability Zones</a>
   */
  REGION(new SimpleConfigurationPropertyBuilder()
      .configKey("rdsRegion")
      .name("RDS region")
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .defaultDescription("ID of the Amazon Web Services region to use for RDS (defaults to configured EC2 region).")
      .defaultPlaceholder("Defaults to EC2 region if not set.")
      .addValidValues(
          "ap-northeast-1",
          "ap-northeast-2",
          "ap-south-1",
          "ap-southeast-1",
          "ap-southeast-2",
          "ca-central-1",
          "eu-central-1",
          "eu-west-1",
          "eu-west-2",
          "eu-west-3",
          "sa-east-1",
          "us-east-1",
          "us-east-2",
          "us-west-1",
          "us-west-2")
      .build()),

  /**
   * <p>Custom endpoint identifying a region.</p>
   * <p>This is critical for Gov. cloud because there is no other way to discover those
   * regions.</p>
   */
  REGION_ENDPOINT(new SimpleConfigurationPropertyBuilder()
      .configKey("rdsRegionEndpoint")
      .name("RDS region endpoint")
      .defaultDescription("<p>RDS region endpoint is an optional URL that Cloudera Altus Director " +
          "can use to communicate with the AWS Relational Database Service.  AWS provides multiple " +
          "regional endpoints for RDS as well as separate GovCloud endpoints. </p>For more " +
          "information see the " +
          "<a target=\"_blank\" href=\"http://docs.aws.amazon.com/general/latest/gr/rande.html#rds_region\">AWS documentation.</a>")
      .defaultPlaceholder("Optionally override the RDS endpoint URL")
      .build());

  /**
   * The configuration property.
   */
  private final ConfigurationProperty configurationProperty;

  /**
   * Creates a configuration property token with the specified parameters.
   *
   * @param configurationProperty the configuration property
   */
  RDSProviderConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
    this.configurationProperty = configurationProperty;
  }

  @Override
  public ConfigurationProperty unwrap() {
    return configurationProperty;
  }
}
