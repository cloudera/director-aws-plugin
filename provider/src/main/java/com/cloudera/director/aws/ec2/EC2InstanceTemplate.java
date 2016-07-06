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

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.AVAILABILITY_ZONE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IAM_PROFILE_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.PLACEMENT_GROUP;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_SIZE_GB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SPOT_BID_USD_PER_HR;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TENANCY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;

import com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.Property;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Represents a template for constructing EC2 compute instances.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class EC2InstanceTemplate extends ComputeInstanceTemplate {

  /**
   * A splitter for comma-separated lists.
   */
  protected static final Splitter CSV_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  /**
   * The list of configuration properties (including inherited properties).
   */
  private static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
      ConfigurationPropertiesUtil.merge(
          ComputeInstanceTemplate.getConfigurationProperties(),
          ConfigurationPropertiesUtil.asConfigurationPropertyList(EC2InstanceTemplateConfigurationPropertyToken.values())
      );

  /**
   * Returns the list of configuration properties for creating an EC2 instance template,
   * including inherited properties.
   *
   * @return the list of configuration properties for creating an EC2 instance template,
   * including inherited properties
   */
  public static List<ConfigurationProperty> getConfigurationProperties() {
    return CONFIGURATION_PROPERTIES;
  }

  /**
   * EC2 compute instance configuration properties.
   */
  // Fully qualifying class name due to compiler bug
  public static enum EC2InstanceTemplateConfigurationPropertyToken
      implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

    /**
     * <p>The availability zone.</p>
     * <p>Multiple availability zones are linked together by high speed low latency connections.
     * Each zone is a distinct failure domain.</p>
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html">Regions and Availability Zones</a>
     */
    AVAILABILITY_ZONE(new SimpleConfigurationPropertyBuilder()
        .configKey("availabilityZone")
        .name("Availability zone")
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .defaultDescription("The availability zone.")
        .hidden(true)
        .build()),

    /**
     * Name of the IAM instance profile.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html">IAM Roles</a>
     */
    IAM_PROFILE_NAME(new SimpleConfigurationPropertyBuilder()
        .configKey("iamProfileName")
        .name("IAM profile name")
        .defaultDescription(
          "The IAM instance profile name.<br />" +
          "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html'>More Information</a>"
        ).build()),

    /**
     * The image (AMI) ID.
     */
    IMAGE(new SimpleConfigurationPropertyBuilder()
        .configKey(ComputeInstanceTemplateConfigurationPropertyToken.IMAGE.unwrap().getConfigKey())
        .name("Image (AMI) ID")
        .required(true)
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .defaultDescription(
            "Must begin with 'ami-'.<br/><br/>" +
            "To learn more about which AMIs should be used, please see the " +
            "<a target='_blank' href='http://www.cloudera.com/content/cloudera/en/documentation/cloudera-director/latest/topics/director_deployment_requirements.html'>Cloudera Director Deployment Requirements Guide</a> " +
            "about what operating systems when deploying a Cloudera Manager or CDH Cluster.<br/><br/>" +
            "See the AWS documentation on " +
            "<a target='_blank' href='https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html'>Amazon Machine Images</a> " +
            "for more information."
        ).defaultErrorMessage("Image (AMI) ID is mandatory")
        .build()),

    /**
     * Name of the key pair to use for new instances.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html">EC2 Key Pairs</a>
     */
    KEY_NAME(new SimpleConfigurationPropertyBuilder()
        .configKey("keyName")
        .name("Key name")
        .required(false)
        .widget(ConfigurationProperty.Widget.TEXT)
        .defaultDescription(
            "The EC2 key pair ID.<br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html'>More Information</a>"
        ).hidden(true)
        .build()),

    /**
     * The placement group name for new instances.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html">Placement Groups</a>
     */
    PLACEMENT_GROUP(new SimpleConfigurationPropertyBuilder()
        .configKey("placementGroup")
        .name("Placement group")
        .defaultDescription("The placement group name should be defined here. " +
          "This setting allows users to launch the desired instance in a defined placement group. " +
          "Instances using placement groups will deploy in a specific zone within a region which helps network performance. <br/>" +
          "<a target='_blank' href='with: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html'>More information</a>")
        .build()),

    /**
     * Size of the root partition in GBs.
     */
    ROOT_VOLUME_SIZE_GB(new SimpleConfigurationPropertyBuilder()
        .configKey("rootVolumeSizeGB")
        .name("Root volume size (GB)")
        .defaultValue("50")
        .defaultDescription(
          "Specify a size for the root volume (in GB). " +
          "Cloudera Director will automatically expand the filesystem so that you can use all " +
          "the available disk space for your application.<br />" +
          "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage_expand_partition.html'>More Information</a>"
        ).widget(ConfigurationProperty.Widget.NUMBER)
        .type(Property.Type.INTEGER)
        .build()),

    /**
     * The type of EBS volume to use for the root drive.
     *
     * @see <a href="http://aws.amazon.com/ebs/details/">EBS</a>
     */
    ROOT_VOLUME_TYPE(new SimpleConfigurationPropertyBuilder()
        .configKey("rootVolumeType")
        .name("Root volume type")
        .defaultValue("gp2")
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .defaultDescription(
          "Specify the type of the EBS volume used for the root partition. The choices for AWS are gp2 and standard. Defaults to gp2.<br/>" +
          "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html'>More Information</a>"
        ).addValidValues(
            "gp2",
            "standard")
        .build()),

    /**
     * The IDs of the VPC security groups (comma separated).
     *
     * @see <a href="http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_SecurityGroups.html">Security Groups</a>
     */
    SECURITY_GROUP_IDS(new SimpleConfigurationPropertyBuilder()
        .configKey("securityGroupsIds")
        .name("Security group IDs")
        .widget(ConfigurationProperty.Widget.OPENMULTI)
        .required(true)
        .defaultDescription(
          "Specify the list of security group IDs. Must begin with 'sg-'.<br />" +
          "<a target='_blank' href='http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_SecurityGroups.html'>More Information</a>"
        ).defaultErrorMessage("VPC security group IDs are mandatory")
        .build()),

    /**
     * Spot bid in USD/hr.
     */
    SPOT_BID_USD_PER_HR(new SimpleConfigurationPropertyBuilder()
        .configKey("spotBidUSDPerHr")
        .name("Spot bid (USD/hr)")
        .defaultDescription(
            "Specify a Spot bid (in USD/hr). " +
                "The Spot bid is the amount you are willing to pay per hour for each Spot Instance. " +
                "Requests for Spot Instances will not be granted if the Spot bid is below " +
                "the current Spot market price, and Spot Instances will be terminated if " +
                "the Spot market price rises above the Spot bid. " +
                "The Spot bid is required if you are using Spot Instances.<br />" +
                "<a target='_blank' href='https://aws.amazon.com/ec2/spot/bid-advisor/'>More Information</a>"
        ).widget(ConfigurationProperty.Widget.NUMBER)
        .type(Property.Type.DOUBLE)
        .defaultErrorMessage("Spot bid is mandatory when using Spot Instances")
        .build()),

    /**
     * The ID of the Amazon VPC subnet.
     *
     * @see <a href="http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Subnets.html">Subnets</a>
     */
    SUBNET_ID(new SimpleConfigurationPropertyBuilder()
        .configKey("subnetId")
        .name("VPC subnet ID")
        .required(true)
        .defaultDescription(
          "The VPC subnet ID.<br />" +
          "<a target='_blank' href='http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Subnets.html'>More Information</a>"
        ).defaultErrorMessage("VPC subnet ID is mandatory")
        .build()),

    /**
     * <p>The tenancy.</p>
     * <p>The tenancy of the instance (if the instance is running in a VPC).
     * An instance with dedicated tenancy runs on single-tenant hardware.</p>
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/how-dedicated-hosts-work.html">Using Dedicated Hosts</a>
     */
    TENANCY(new SimpleConfigurationPropertyBuilder()
            .configKey("tenancy")
            .name("Tenancy")
            .defaultValue("default")
            .widget(ConfigurationProperty.Widget.LIST)
            .defaultDescription(
              "Instance tenancy should be defined here. This setting allows users to launch the desired instance in VPC with a defined tenancy. <br/>" +
              "<a target='_blank' href='http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/dedicated-instance.html'>More information</a>"
            ).addValidValues(
              "default",
              "dedicated"
            )
            .build()),

    /**
     * The instance type (e.g. t1.micro, m1.medium, etc.).
     */
    TYPE(new SimpleConfigurationPropertyBuilder()
        .configKey(ComputeInstanceTemplateConfigurationPropertyToken.TYPE.unwrap().getConfigKey())
        .name("Instance type")
        .required(true)
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .defaultDescription(
          "Select an instance type as per the needs of the processes to be run on this instance.<br />" +
          "<a target='_blank' href='http://aws.amazon.com/ec2/instance-types/'>More Information</a>"
        ).defaultErrorMessage("Instance type is mandatory")
        .addValidValues(
            "t2.micro",
            "t2.small",
            "t2.medium",
            "t2.large",
            "m3.medium",
            "m3.large",
            "m3.xlarge",
            "m3.2xlarge",
            "m4.large",
            "m4.xlarge",
            "m4.2xlarge",
            "m4.4xlarge",
            "m4.10xlarge",
            "c3.large",
            "c3.xlarge",
            "c3.2xlarge",
            "c3.4xlarge",
            "c3.8xlarge",
            "c4.large",
            "c4.xlarge",
            "c4.2xlarge",
            "c4.4xlarge",
            "c4.8xlarge",
            "g2.2xlarge",
            "r3.large",
            "r3.xlarge",
            "r3.2xlarge",
            "r3.4xlarge",
            "r3.8xlarge",
            "i2.xlarge",
            "i2.2xlarge",
            "i2.4xlarge",
            "i2.8xlarge",
            "hs1.8xlarge",
            "d2.xlarge",
            "d2.2xlarge",
            "d2.4xlarge",
            "d2.8xlarge")
        .build()),

    /**
     * Whether to use Spot Instances. Default is <code>false</code>.
     *
     * @see <a href="http://aws.amazon.com/ec2/spot/">Spot Instances</a>
     */
    USE_SPOT_INSTANCES(new SimpleConfigurationPropertyBuilder()
        .configKey("useSpotInstances")
        .name("Use Spot Instances")
        .widget(ConfigurationProperty.Widget.CHECKBOX)
        .defaultValue("false")
        .type(Property.Type.BOOLEAN)
        .defaultDescription("Whether to use Spot Instances. " +
            "Since Spot Instances can be terminated unexpectedly if the Spot market price increases, " +
            "they should be used only for workers, and not for nodes that must be reliable, " +
            "such as masters and data nodes.<br />" +
            "<a target='_blank' href='http://aws.amazon.com/ec2/spot/'>More Information</a>")
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
    private EC2InstanceTemplateConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    @Override
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }
  }

  /**
   * The instance type.
   */
  private final String type;

  /**
   * The image (AMI) ID.
   */
  private final String image;

  /**
   * The availability zone.
   */
  private final Optional<String> availabilityZone;

  /**
   * The placement group.
   */
  private final Optional<String> placementGroup;

  /**
   * The tenancy.
   */
  private final String tenancy;

  /**
   * The subnet ID.
   */
  private final String subnetId;

  /**
   * The security group IDs.
   */
  private final List<String> securityGroupIds;

  /**
   * The root volume size (in GB).
   */
  private final int rootVolumeSizeGB;

  /**
   * The root volume type.
   */
  private final String rootVolumeType;

  /**
   * The optional IAM profile name.
   */
  private final Optional<String> iamProfileName;

  /**
   * The optional EC2 key pair name.
   */
  private final Optional<String> keyName;

  /**
   * Whether to use Spot Instances.
   */
  private final boolean useSpotInstances;

  /**
   * The Spot bid, in USD/hr.
   */
  private final Optional<BigDecimal> spotBidUSDPerHour;

  /**
   * Creates an EC2 instance template with the specified parameters.
   *
   * @param name                        the name of the template
   * @param configuration               the source of configuration
   * @param tags                        the map of tags to be applied to resources created from the template
   * @param providerLocalizationContext the parent provider localization context
   */
  public EC2InstanceTemplate(String name, Configured configuration, Map<String, String> tags,
      LocalizationContext providerLocalizationContext) {
    super(name, configuration, tags, providerLocalizationContext);
    LocalizationContext localizationContext = getLocalizationContext();
    this.type = getConfigurationValue(TYPE, localizationContext);
    this.image = getConfigurationValue(IMAGE, localizationContext);

    this.subnetId = getConfigurationValue(SUBNET_ID, localizationContext);
    this.securityGroupIds =
        CSV_SPLITTER.splitToList(getConfigurationValue(SECURITY_GROUP_IDS, localizationContext));

    this.availabilityZone =
        Optional.fromNullable(getConfigurationValue(AVAILABILITY_ZONE, localizationContext));
    this.placementGroup =
        Optional.fromNullable(getConfigurationValue(PLACEMENT_GROUP, localizationContext));
    this.tenancy = getConfigurationValue(TENANCY, localizationContext);

    this.rootVolumeSizeGB =
        Integer.parseInt(getConfigurationValue(ROOT_VOLUME_SIZE_GB, localizationContext));
    this.rootVolumeType = getConfigurationValue(ROOT_VOLUME_TYPE, localizationContext);

    this.iamProfileName =
        Optional.fromNullable(getConfigurationValue(IAM_PROFILE_NAME, localizationContext));
    this.keyName =
        Optional.fromNullable(getConfigurationValue(KEY_NAME, localizationContext));

    boolean useSpotInstances =
        Boolean.parseBoolean(getConfigurationValue(USE_SPOT_INSTANCES, localizationContext));
    this.useSpotInstances = useSpotInstances;
    String spotBidUSDPerHourString = getConfigurationValue(SPOT_BID_USD_PER_HR, localizationContext);
    this.spotBidUSDPerHour = useSpotInstances
        ? Optional.of(new BigDecimal(spotBidUSDPerHourString))
        : Optional.<BigDecimal>absent();
  }

  /**
   * Returns the instance type.
   *
   * @return the instance type
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the image (AMI) ID.
   *
   * @return the image (AMI) ID
   */
  public String getImage() {
    return image;
  }

  /**
   * Returns the optional availability zone.
   *
   * @return the optional availability zone
   */
  public Optional<String> getAvailabilityZone() {
    return availabilityZone;
  }

  /**
   * Returns the optional placement group.
   *
   * @return the optional placement group
   */
  public Optional<String> getPlacementGroup() {
    return placementGroup;
  }

  /**
   * Returns tenancy.
   *
   * @return tenancy
   */
  public String getTenancy() { return tenancy; }

    /**
     * Returns the subnet ID.
     *
     * @return the subnet ID
     */
  public String getSubnetId() {
    return subnetId;
  }

  /**
   * Returns the security group IDs.
   *
   * @return the security group IDs
   */
  public List<String> getSecurityGroupIds() {
    return securityGroupIds;
  }

  /**
   * Returns the root volume size (in GB).
   *
   * @return the root volume size (in GB)
   */
  public int getRootVolumeSizeGB() {
    return rootVolumeSizeGB;
  }

  /**
   * Returns the root volume type.
   *
   * @return the root volume type
   */
  public String getRootVolumeType() {
    return rootVolumeType;
  }

  /**
   * Returns the optional IAM profile name.
   *
   * @return the optional IAM profile name
   */
  public Optional<String> getIamProfileName() {
    return iamProfileName;
  }

  /**
   * Returns the optional EC2 key pair name.
   *
   * @return the optional EC2 key pair name
   */
  public Optional<String> getKeyName() {
    return keyName;
  }

  /**
   * Returns whether to use Spot Instances.
   *
   * @return whether to use Spot Instances
   */
  public boolean isUseSpotInstances() {
    return useSpotInstances;
  }

  /**
   * Returns the Spot bid, in USD/hr.
   *
   * @return the Spot bid, in USD/hr
   */
  public Optional<BigDecimal> getSpotBidUSDPerHour() {
    return spotBidUSDPerHour;
  }
}
