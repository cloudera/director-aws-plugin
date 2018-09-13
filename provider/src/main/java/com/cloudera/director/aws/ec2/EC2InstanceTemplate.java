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

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ALLOCATE_EBS_SEPARATELY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.AUTOMATIC_INSTANCE_PROCESSING;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.AVAILABILITY_ZONE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.BLOCK_DURATION_MINUTES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_IOPS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_KMS_KEY_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_OPTIMIZED;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_COUNT;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_SIZE_GIB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.EBS_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ENCRYPT_ADDITIONAL_EBS_VOLUMES;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IAM_PROFILE_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.IMAGE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.PLACEMENT_GROUP;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_SIZE_GB;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.ROOT_VOLUME_TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SECURITY_GROUP_IDS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SPOT_BID_USD_PER_HR;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SPOT_PRICE_USD_PER_HR;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SUBNET_ID;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.SYSTEM_DISKS;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TENANCY;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USER_DATA;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USER_DATA_UNENCODED;
import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.USE_SPOT_INSTANCES;

import com.cloudera.director.aws.ec2.ebs.SystemDisk;
import com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.Property;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v2.util.ConfigurationPropertiesUtil;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Represents a template for constructing EC2 compute instances.
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "Guava"})
public class EC2InstanceTemplate extends ComputeInstanceTemplate {

  /**
   * A base 64 encoder for user data.
   */
  private static final BaseEncoding BASE64 = BaseEncoding.base64();

  /**
   * A splitter for comma-separated lists.
   */
  public static final Splitter CSV_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

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
  public enum EC2InstanceTemplateConfigurationPropertyToken
      implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

    /**
     * Whether to allow AWS Auto Scaling to automatically perform certain operations on Auto Scaling
     * instances, such as replacing those that fail health checks or rebalancing availability zones.
     *
     * @see <a href="http://docs.aws.amazon.com/autoscaling/ec2/userguide/as-suspend-resume-processes.html">Suspending and Resuming Scaling Processes</a>
     */
    AUTOMATIC_INSTANCE_PROCESSING(new SimpleConfigurationPropertyBuilder()
        .configKey("enableAutomaticInstanceProcesing")
        .name("Enable automatic instance processing")
        .defaultValue("false")
        .type(Property.Type.BOOLEAN)
        .defaultDescription(
            "Whether to allow AWS Auto Scaling to automatically perform certain operations on " +
                "Auto Scaling instances, such as replacing those that fail health checks or " +
                "rebalancing availability zones.<br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/autoscaling/ec2/userguide/as-suspend-resume-processes.html'>More Information</a>"
        ).widget(ConfigurationProperty.Widget.CHECKBOX)
        .hidden(true)
        .build()),

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
     * Whether to enable EBS Optimization.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSOptimized.html">EBS–Optimized Instances</a>
     */
    EBS_OPTIMIZED(new SimpleConfigurationPropertyBuilder()
        .configKey("ebsOptimized")
        .name("EBS Optimized")
        .type(Property.Type.BOOLEAN)
        .defaultValue("false")
        .widget(ConfigurationProperty.Widget.CHECKBOX)
        .defaultDescription(
            "Specify whether to enable EBS Optimized I/O. This optimization isn't available with all instance types. " +
                "Some instance types are EBS Optimized by default regardless of this flag. Additional usage charges " +
                "may apply when using an EBS-optimized instance<br/>" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSOptimized.html'>More Information</a>"
        )
        .build()),

    /**
     * Number of additional EBS volumes to mount.
     */
    EBS_VOLUME_COUNT(new SimpleConfigurationPropertyBuilder()
        .configKey("ebsVolumeCount")
        .name("EBS Volume Count")
        .defaultValue("0")
        .defaultDescription(
            "The number of additional EBS volumes to mount. Cloudera Altus Director will create and attach these volumes to the " +
                "provisioned instance. These added volumes will be deleted when the instance is terminated from Director. <br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumes.html'>More Information</a>"
        ).widget(ConfigurationProperty.Widget.NUMBER)
        .type(Property.Type.INTEGER)
        .build()),

    /**
     * The size of the additional EBS volume in GiB.
     */
    EBS_VOLUME_SIZE_GIB(new SimpleConfigurationPropertyBuilder()
        .configKey("ebsVolumeSizeGiB")
        .name("EBS Volume Size (GiB)")
        .defaultValue("500")
        .defaultDescription(
            "The size of the additional EBS volume(s) in GiB."
        ).widget(ConfigurationProperty.Widget.NUMBER)
        .type(Property.Type.INTEGER)
        .build()),

    /**
     * The volume type of the additional EBS volumes.
     */
    EBS_VOLUME_TYPE(new SimpleConfigurationPropertyBuilder()
        .configKey("ebsVolumeType")
        .name("EBS Volume Type")
        .addValidValues("st1", "sc1", "gp2", "io1")
        .defaultValue("st1")
        .defaultDescription(
            "The EBS volume type for the additional EBS volumes. Supported volumes are Throughput Optimized HDD (st1), " +
                "Cold HDD (sc1), General Purpose SSD (gp2) and Provisioned IOPS SSD (io1) <br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html'>More Information</a>"
        ).widget(ConfigurationProperty.Widget.OPENLIST)
        .build()),

    /**
     * The number of I/O operations per second (IOPS) when using io1 volume types.
     */
    EBS_IOPS(new SimpleConfigurationPropertyBuilder()
        .configKey("ebsIops")
        .name("EBS IOPS Count")
        .defaultDescription(
            "Only valid for Provisioned IOPS (io1) SSD volumes. The number of I/O operations per second (IOPS) to " +
                "provision for the volume."
        ).widget(ConfigurationProperty.Widget.NUMBER)
        .type(Property.Type.INTEGER)
        .build()),

    /**
     * Configurations for extra EBS volumes to mount, this allows for heterogeneous volumes.
     * Note that this is a temporary solution and this configuration configuration property
     * may be removed in the future.
     */
    SYSTEM_DISKS(new SimpleConfigurationPropertyBuilder()
        .configKey("systemDisks")
        .name("Configurations for extra EBS volumes")
        .widget(ConfigurationProperty.Widget.OPENMULTI)
        .hidden(true)
        .defaultDescription(
            "Configurations for extra EBS volumes"
        )
        .build()),

    /**
     * Whether to enable ebs encryption of the additional EBS volumes.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSEncryption.html">EBS Encryption</a>
     */
    ENCRYPT_ADDITIONAL_EBS_VOLUMES(new SimpleConfigurationPropertyBuilder()
        .configKey("enableEbsEncryption")
        .name("Enable EBS Encryption")
        .defaultValue("false")
        .type(Property.Type.BOOLEAN)
        .defaultDescription(
            "Whether to enable encryption for the additional EBS volumes. Note that encryption does not apply to the root volume.<br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSEncryption.html'>More Information</a>"
        ).widget(ConfigurationProperty.Widget.CHECKBOX)
        .build()),

    /**
     * The CMK to use for encryption of the additional EBS volumes.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSEncryption.html">EBS Encryption</a>
     */
    EBS_KMS_KEY_ID(new SimpleConfigurationPropertyBuilder()
        .configKey("ebsKmsKeyId")
        .name("EBS KMS Key ID")
        .defaultDescription(
            "The full ARN of the KMS CMK to use when encrypting volumes. If encryption is enabled and this " +
                "is blank, the default CMK will be used. Note that encryption does not apply to the root volume.<br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSEncryption.html'>More Information</a>"
        ).build()),

    /**
     * Whether to allocate EBS volumes separately when using non-default KMS Key ID.
     */
    ALLOCATE_EBS_SEPARATELY(new SimpleConfigurationPropertyBuilder()
        .configKey("allocateEbsSeparately")
        .name("Allocate EBS volumes separately")
        .defaultValue("false")
        .type(Property.Type.BOOLEAN)
        .defaultDescription(
            "Whether to allocate EBS volumes separately when using non-default KMS Key ID. This should" +
                "only be enabled on certain regions (like GovCloud) that don't allow specifying EBS KMS" +
                "Key ID as part of the instance launch request."
        ).widget(ConfigurationProperty.Widget.CHECKBOX)
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
                "<a target='_blank' href='http://www.cloudera.com/content/cloudera/en/documentation/cloudera-director/latest/topics/director_deployment_requirements.html'>Cloudera Altus Director Deployment Requirements Guide</a> " +
                "about what operating systems when deploying a Cloudera Manager or CDH Cluster.<br/><br/>" +
                "See the AWS documentation on " +
                "<a target='_blank' href='https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html'>Amazon Machine Images</a> " +
                "for more information."
        ).defaultPlaceholder("Enter or select an image")
        .defaultErrorMessage("Image (AMI) ID is mandatory")
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
            "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html'>More information</a>")
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
                "Cloudera Altus Director will automatically expand the filesystem so that you can use all " +
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
            "Since Spot Instances can be terminated unexpectedly if the EC2 Spot price increases, " +
            "they should be used only for workers, and not for nodes that must be reliable, " +
            "such as masters and data nodes.<br />" +
            "<a target='_blank' href='http://aws.amazon.com/ec2/spot/'>More Information</a>")
        .build()),

    /**
     * Spot bid in USD/hr.
     *
     * @deprecated Use {@link #SPOT_PRICE_USD_PER_HR} instead.
     */
    SPOT_BID_USD_PER_HR(new SimpleConfigurationPropertyBuilder()
        .configKey("spotBidUSDPerHr")
        .name("Spot bid (USD/hr)")
        .defaultDescription(
            "Specify a Spot bid (in USD/hr). " +
                "This property is deprecated in favor of spotPriceUSDPerHr, but still works as " +
                "an alternative name for spotPriceUSDPerHr. This property will be removed in a " +
                "future plugin release."
        ).widget(ConfigurationProperty.Widget.NUMBER)
        .type(Property.Type.DOUBLE)
        .hidden(true)
        .build()),

    /**
     * Spot price in USD/hr.
     */
    SPOT_PRICE_USD_PER_HR(new SimpleConfigurationPropertyBuilder()
        .configKey("spotPriceUSDPerHr")
        .name("Spot price (USD/hr)")
        .defaultDescription(
            "Specify a Spot price (in USD/hr). " +
                "The Spot price is the maximum amount you are willing to pay per hour for each Spot Instance. " +
                "When your Spot price exceeds the Spot price set by EC2, then EC2 fulfills your request, " +
                "if capacity is available. Spot Instances will be terminated if the Spot price set by EC2 " +
                "rises above your Spot price."
        ).widget(ConfigurationProperty.Widget.NUMBER)
        .type(Property.Type.DOUBLE)
        .build()),

    /**
     * The required duration for the Spot instances.
     *
     * @see <a href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-requests.html#fixed-duration-spot-instances'>Fixed Duration Spot Instances</a>
     */
    BLOCK_DURATION_MINUTES(new SimpleConfigurationPropertyBuilder()
        .configKey("blockDurationMinutes")
        .name("Spot Block Duration (minutes)")
        .defaultDescription(
            "Specify the required duration for the Spot instances (also known as Spot blocks), " +
                "in minutes. This value must be a multiple of 60 (60, 120, 180, 240, 300, or 360). " +
                "Spot block instances will run for the predefined duration - in hourly increments " +
                "up to six hours in length - at a significant discount compared to On-Demand " +
                "pricing.<br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-requests.html#fixed-duration-spot-instances'>More Information</a>"
        ).widget(ConfigurationProperty.Widget.LIST)
        .type(Property.Type.INTEGER)
        .addValidValues("60", "120", "180", "240", "300", "360")
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
        ).defaultPlaceholder("Select an instance type")
        .defaultErrorMessage("Instance type is mandatory")
        .addValidValues(
            "t2.micro",
            "t2.small",
            "t2.medium",
            "t2.large",
            "t2.xlarge",
            "t2.2xlarge",
            "m3.medium",
            "m3.large",
            "m3.xlarge",
            "m3.2xlarge",
            "m4.large",
            "m4.xlarge",
            "m4.2xlarge",
            "m4.4xlarge",
            "m4.10xlarge",
            "m4.16xlarge",
            "m5.large",
            "m5.xlarge",
            "m5.2xlarge",
            "m5.4xlarge",
            "m5.12xlarge",
            "m5.24xlarge",
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
            "c5.large",
            "c5.xlarge",
            "c5.2xlarge",
            "c5.4xlarge",
            "c5.9xlarge",
            "c5.18xlarge",
            "g2.2xlarge",
            "g2.8xlarge",
            "g3.4xlarge",
            "g3.8xlarge",
            "g3.16xlarge",
            "p2.xlarge",
            "p2.8xlarge",
            "p2.16xlarge",
            "p3.2xlarge",
            "p3.8xlarge",
            "p3.16xlarge",
            "r3.large",
            "r3.xlarge",
            "r3.2xlarge",
            "r3.4xlarge",
            "r3.8xlarge",
            "r4.large",
            "r4.xlarge",
            "r4.2xlarge",
            "r4.4xlarge",
            "r4.8xlarge",
            "r4.16xlarge",
            "x1.16xlarge",
            "x1.32xlarge",
            "i2.xlarge",
            "i2.2xlarge",
            "i2.4xlarge",
            "i2.8xlarge",
            "i3.large",
            "i3.xlarge",
            "i3.2xlarge",
            "i3.4xlarge",
            "i3.8xlarge",
            "i3.16xlarge",
            "h1.2xlarge",
            "h1.4xlarge",
            "h1.8xlarge",
            "h1.16xlarge",
            "hs1.8xlarge",
            "d2.xlarge",
            "d2.2xlarge",
            "d2.4xlarge",
            "d2.8xlarge",
            "f1.2xlarge",
            "f1.16xlarge")
        .build()),

    /**
     * The user data, base64 encoded. Mutually exclusive with {@link #USER_DATA_UNENCODED}.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html">Instance Metadata and User Data</a>
     */
    USER_DATA(new SimpleConfigurationPropertyBuilder()
        .configKey("userData")
        .name("User data")
        .required(false)
        .widget(ConfigurationProperty.Widget.TEXT)
        .defaultDescription(
            "The user data, base64 encoded.<br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html'>More Information</a>"
        )
        .build()),

    /**
     * The user data, unencoded. Mutually exclusive with {@link #USER_DATA}.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html">Instance Metadata and User Data</a>
     */
    USER_DATA_UNENCODED(new SimpleConfigurationPropertyBuilder()
        .configKey("userDataUnencoded")
        .name("User data (unencoded)")
        .required(false)
        .widget(ConfigurationProperty.Widget.TEXTAREA)
        .defaultDescription(
            "The user data, unencoded.<br />" +
                "<a target='_blank' href='http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html'>More Information</a>"
        )
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
    EC2InstanceTemplateConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
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
   * Whether to enable EBS Optimization.
   */
  private final boolean ebsOptimized;

  /**
   * Number of additional EBS volumes to mount.
   */
  private final int ebsVolumeCount;

  /**
   * The size of the additional EBS volume in GiB.
   */
  private final int ebsVolumeSizeGiB;

  /**
   * The volume type of the additional EBS volumes.
   */
  private final String ebsVolumeType;

  /**
   * The number of I/O operations per second (IOPS) when using io1 volume types.
   */
  private final Optional<Integer> ebsIops;

  /**
   * Configuration for extra EBS volumes.
   */
  private final List<SystemDisk> systemDisks;

  /**
   * Whether to enable ebs encryption of the additional EBS volumes.
   */
  private final boolean enableEbsEncryption;

  /**
   * The optional CMK to use for encryption of the additional EBS volumes.
   */
  private final Optional<String> ebsKmsKeyId;

  /**
   * Whether to allocate EBS volumes separately when using non-default KMS Key ID.
   */
  private final boolean allocateEbsSeparately;

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
   * The Spot price, in USD/hr.
   */
  private final Optional<BigDecimal> spotPriceUSDPerHour;

  /**
   * The block duration in minutes for Spot block instances.
   */
  private final Optional<Integer> blockDurationMinutes;

  /**
   * The user data, base64 encoded.
   */
  private final Optional<String> userData;

  /**
   * Whether to allow AWS Auto Scaling to automatically perform certain operations on Auto Scaling
   * instances, such as replacing those that fail health checks or rebalancing availability zones.
   */
  private final boolean enableAutomaticInstanceProcesing;

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

    this.ebsOptimized = Boolean.parseBoolean(getConfigurationValue(EBS_OPTIMIZED, localizationContext));

    this.ebsVolumeCount = Integer.parseInt(getConfigurationValue(EBS_VOLUME_COUNT, localizationContext));
    this.ebsVolumeSizeGiB = Integer.parseInt(getConfigurationValue(EBS_VOLUME_SIZE_GIB, localizationContext));
    this.ebsVolumeType = getConfigurationValue(EBS_VOLUME_TYPE, localizationContext);

    Optional<String> strEbsIops = Optional.fromNullable(getConfigurationValue(EBS_IOPS, localizationContext));
    this.ebsIops = strEbsIops.isPresent() ?
        Optional.of(Integer.parseInt(strEbsIops.get())) : Optional.absent();

    this.enableEbsEncryption =
        Boolean.parseBoolean(getConfigurationValue(ENCRYPT_ADDITIONAL_EBS_VOLUMES, localizationContext));
    this.ebsKmsKeyId = Optional.fromNullable(getConfigurationValue(EBS_KMS_KEY_ID, localizationContext));

    this.allocateEbsSeparately =
        Boolean.parseBoolean(getConfigurationValue(ALLOCATE_EBS_SEPARATELY, localizationContext));

    this.systemDisks =
        SystemDisk.parse(getConfigurationValue(SYSTEM_DISKS, localizationContext));

    this.iamProfileName =
        Optional.fromNullable(getConfigurationValue(IAM_PROFILE_NAME, localizationContext));
    this.keyName =
        Optional.fromNullable(getConfigurationValue(KEY_NAME, localizationContext));

    boolean useSpotInstances =
        Boolean.parseBoolean(getConfigurationValue(USE_SPOT_INSTANCES, localizationContext));
    this.useSpotInstances = useSpotInstances;
    Optional<String> spotPriceUSDPerHourString =
        Optional.fromNullable(getConfigurationValue(SPOT_PRICE_USD_PER_HR, localizationContext))
            .or(Optional.fromNullable(getConfigurationValue(SPOT_BID_USD_PER_HR, localizationContext)));
    this.spotPriceUSDPerHour = useSpotInstances && spotPriceUSDPerHourString.isPresent()
        ? Optional.of(new BigDecimal(spotPriceUSDPerHourString.get()))
        : Optional.absent();
    String blockDurationMinutesString = Strings.emptyToNull(
        getConfigurationValue(BLOCK_DURATION_MINUTES, localizationContext));
    this.blockDurationMinutes = useSpotInstances && blockDurationMinutesString != null
        ? Optional.of(Integer.parseInt(blockDurationMinutesString))
        : Optional.absent();

    this.userData =
        Optional.fromNullable(getConfigurationValue(USER_DATA, localizationContext))
            .or(Optional.fromNullable(base64Encode(getConfigurationValue(USER_DATA_UNENCODED, localizationContext))));

    this.enableAutomaticInstanceProcesing =
        Boolean.parseBoolean(getConfigurationValue(AUTOMATIC_INSTANCE_PROCESSING, localizationContext));
  }

  private String base64Encode(String s) {
    if (s == null) {
      return null;
    }
    return BASE64.encode(s.getBytes(StandardCharsets.UTF_8));
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
  public String getTenancy() {
    return tenancy;
  }

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
   * Whether to enable EBS Optimization.
   *
   * @return whether to enable EBS Optimization
   */
  public boolean isEbsOptimized() {
    return ebsOptimized;
  }

  /**
   * Returns the EBS volume count.
   *
   * @return the EBS volume count
   */
  public int getEbsVolumeCount() {
    return ebsVolumeCount;
  }

  /**
   * Returns the EBS volume size in GiB.
   *
   * @return the EBS volume size in GiB
   */
  public int getEbsVolumeSizeGiB() {
    return ebsVolumeSizeGiB;
  }

  /**
   * Returns the EBS volume type.
   *
   * @return the EBS volume type.
   */
  public String getEbsVolumeType() {
    return ebsVolumeType;
  }

  /**
   * Returns the number of I/O operations per second (IOPS).
   *
   * @return the number of I/O operations per second (IOPS).
   */
  public Optional<Integer> getEbsIops() {
    return ebsIops;
  }

  /**
   * Returns whether to enable EBS encryption.
   *
   * @return whether to enable EBS encryption.
   */
  public boolean isEnableEbsEncryption() {
    return enableEbsEncryption;
  }

  /**
   * Returns the optional CMK to use for encryption of the additional EBS volumes.
   *
   * @return the optional CMK to use for encryption of the additional EBS volumes.
   */
  public Optional<String> getEbsKmsKeyId() {
    return ebsKmsKeyId;
  }

  /**
   * Returns whether to allocate EBS volumes separately when using non-default KMS Key ID.
   *
   * @return whether to allocate EBS volumes separately when using non-default KMS Key ID
   */
  public boolean isAllocateEbsSeparately() {
    return allocateEbsSeparately;
  }

  /**
   * Returns the system disks.
   *
   * @return the system disk.
   */
  public List<SystemDisk> getSystemDisks() {
    return systemDisks;
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
   * Returns the Spot price, in USD/hr.
   *
   * @return the Spot price, in USD/hr
   */
  public Optional<BigDecimal> getSpotPriceUSDPerHour() {
    return spotPriceUSDPerHour;
  }

  /**
   * Returns the Spot block duration, in minutes.
   *
   * @return the Spot block duration, in minutes
   */
  public Optional<Integer> getBlockDurationMinutes() {
    return blockDurationMinutes;
  }

  /**
   * Returns the user data, base64 encoded.
   *
   * @return the user data, base64 encoded
   */
  public Optional<String> getUserData() {
    return userData;
  }

  /**
   * Returns whether to allow AWS Auto Scaling to automatically perform certain operations on Auto Scaling
   * instances, such as replacing those that fail health checks or rebalancing availability zones.
   *
   * @return whether to allow AWS Auto Scaling to automatically perform certain operations on Auto Scaling
   * instances, such as replacing those that fail health checks or rebalancing availability zones
   */
  public boolean isEnableAutomaticInstanceProcesing() {
    return enableAutomaticInstanceProcesing;
  }
}
