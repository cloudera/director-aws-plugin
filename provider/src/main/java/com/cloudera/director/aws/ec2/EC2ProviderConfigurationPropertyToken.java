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

import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Property;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;

/**
 * EC2 configuration properties.
 */
// Fully qualifying class name due to compiler bug
public enum EC2ProviderConfigurationPropertyToken
    implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

  /**
   * <p>Custom endpoint identifying a region for Auto Scaling.</p>
   * <p>This is critical for Gov. cloud because there is no other way to discover those
   * regions.</p>
   */
  AS_REGION_ENDPOINT(new SimpleConfigurationPropertyBuilder()
      .configKey("asRegionEndpoint")
      .name("Auto Scaling region endpoint")
      .defaultDescription("<p>AS region endpoint is an optional URL that Cloudera Altus Director can use to " +
          "communicate with the AWS Auto Scaling service. AWS provides multiple regional endpoints for " +
          "Auto Scaling as well as GovCloud endpoints.</p>For more information see the <a target=\"_blank\" " +
          "href=\"https://docs.aws.amazon.com/general/latest/gr/rande.html#autoscaling_region\">AWS " +
          "documentation.</a>")
      .defaultPlaceholder("Optionally override the auto scaling endpoint URL")
      .build()),

  /**
   * Whether to associate a public IP address with instances. Default is <code>true</code>.
   *
   * @see <a href="http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/vpc-ip-addressing.html">IP Addressing in
   * your VPC</a>
   */
  ASSOCIATE_PUBLIC_IP_ADDRESSES(new SimpleConfigurationPropertyBuilder()
      .configKey("associatePublicIpAddresses")
      .name("Associate public IP addresses")
      .widget(ConfigurationProperty.Widget.CHECKBOX)
      .defaultValue("true")
      .type(Property.Type.BOOLEAN)
      .defaultDescription("Whether to associate a public IP address with instances or not. " +
          "If this is false, instances are expected to be able to access the internet using a NAT instance. " +
          "Currently the only way to get optimal S3 data transfer performance is to assign " +
          "public IP addresses to instances and not use NAT instances (public subnet setup).")
      .build()),

  /**
   * A custom IAM endpoint URL. When not specified, the global IAM endpoint is used.
   *
   * @see <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html#iam_region">IAM endpoints</a>
   */
  IAM_ENDPOINT(new SimpleConfigurationPropertyBuilder()
      .configKey("iamEndpoint")
      .name("IAM endpoint")
      .defaultDescription("<p>IAM endpoint is an optional URL that Cloudera Altus Director can use to communicate with" +
          " the AWS Identity and Access Management service.  AWS provides a single endpoint for IAM.</p>For more" +
          " information see the <a target=\"_blank\" href=" +
          "\"https://docs.aws.amazon.com/general/latest/gr/rande.html#iam_region\">AWS documentation.</a>")
      .defaultPlaceholder("Optionally override the IAM endpoint URL")
      .build()),

  /**
   * Whether to import key pair to AWS if it's missing. Default is <code>false</code>.
   *
   * @see <a href="http://docs.aws.amazon.com/cli/latest/reference/ec2/import-key-pair.html">Importing key pair
   * to your AWS account</a>
   */
  IMPORT_KEY_PAIR_IF_MISSING(new SimpleConfigurationPropertyBuilder()
      .configKey("importKeyPairIfMissing")
      .name("Import key pair if missing")
      .widget(ConfigurationProperty.Widget.CHECKBOX)
      .defaultValue("false")
      .type(Property.Type.BOOLEAN)
      .defaultDescription("<p>Whether to import missing key pair to your EC2 account. The public key is " +
          "extracted from PEM encoding of the private key supplied in the request.</p>For more information see the " +
          "<a target=\"_blank\" href=\"https://docs.aws.amazon.com/cli/latest/reference/ec2/import-key-pair.html\">" +
          "AWS documentation.</a>")
      .build()),

  /**
   * Prefix for key names used when importing missing key pairs. Default is "CLOUDERA-".
   *
   * @see <a href="http://docs.aws.amazon.com/cli/latest/reference/ec2/import-key-pair.html">Importing key pair
   * to your AWS account</a>
   */
  KEY_NAME_PREFIX(new SimpleConfigurationPropertyBuilder()
      .configKey("keyNamePrefix")
      .name("Prefix for key names used in import")
      .defaultValue("CLOUDERA-")
      .defaultDescription("<p>Prefix used for generated key names used when importing missing key pairs.</p>")
      .build()),

  /**
   * <p>Custom endpoint identifying a region for KMS.</p>
   * <p>This is critical for Gov. cloud because there is no other way to discover those
   * regions.</p>
   */
  KMS_REGION_ENDPOINT(new SimpleConfigurationPropertyBuilder()
      .configKey("kmsRegionEndpoint")
      .name("KMS region endpoint")
      .defaultDescription("<p>KMS region endpoint is an optional URL that Cloudera Altus Director can use to " +
          "communicate with the AWS KMS service. AWS provides multiple regional endpoints for KMS " +
          "as well as GovCloud endpoints.</p>For more information see the <a target=\"_blank\" " +
          "href=\"https://docs.aws.amazon.com/general/latest/gr/rande.html#kms_region\">AWS " +
          "documentation.</a>")
      .defaultPlaceholder("Optionally override the KMS endpoint URL")
      .build()),

  /**
   * <p>Custom endpoint identifying a region for STS.</p>
   * <p>This is critical for Gov. cloud because there is no other way to discover those
   * regions.</p>
   */
  STS_REGION_ENDPOINT(new SimpleConfigurationPropertyBuilder()
      .configKey("stsRegionEndpoint")
      .name("STS region endpoint")
      .defaultDescription("<p>STS endpoint is an optional URL that Cloudera Altus Director can use to " +
          "communicate with the AWS STS service. AWS provides multiple regional endpoints for STS. " +
          "as well as GovCloud endpoints.</p>For more information see the <a target=\"_blank\" " +
          "href=\"https://docs.aws.amazon.com/general/latest/gr/rande.html#sts_region\">AWS " +
          "documentation.</a>")
      .defaultPlaceholder("Optionally override the STS endpoint URL")
      .build()),

  /**
   * EC2 region. Each region is a separate geographic area. Each region has multiple, isolated
   * locations known as Availability Zones. A defaultPlaceholder message has been added in lieu
   * of a default value.
   *
   * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html">Regions
   * and Availability Zones</a>
   */
  REGION(new SimpleConfigurationPropertyBuilder()
      .configKey("region")
      .name("EC2 region")
      .defaultDescription("ID of the Amazon Web Services region to use.")
      .defaultPlaceholder("Please select a region")
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .addValidValues(
          "ap-east-1",
          "ap-northeast-1",
          "ap-northeast-2",
          "ap-south-1",
          "ap-southeast-1",
          "ap-southeast-2",
          "ca-central-1",
          "eu-central-1",
          "eu-north-1",
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
      .configKey("regionEndpoint")
      .name("EC2 region endpoint")
      .defaultDescription("<p>EC2 region endpoint is an optional URL that Cloudera Altus Director can use to " +
          "communicate with the AWS EC2 service.  AWS provides multiple regional endpoints for " +
          "EC2 as well as GovCloud endpoints.</p>For more information see the <a target=\"_blank\" " +
          "href=\"https://docs.aws.amazon.com/general/latest/gr/rande.html#ec2_region\">AWS " +
          "documentation.</a>")
      .defaultPlaceholder("Optionally override the EC2 endpoint URL")
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
  EC2ProviderConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
    this.configurationProperty = configurationProperty;
  }

  @Override
  public ConfigurationProperty unwrap() {
    return configurationProperty;
  }
}
