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

import com.amazonaws.services.ec2.model.GroupIdentifier;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceLifecycleType;
import com.cloudera.director.spi.v2.compute.VirtualizationType;
import com.cloudera.director.spi.v2.compute.util.AbstractComputeInstance;
import com.cloudera.director.spi.v2.model.DisplayProperty;
import com.cloudera.director.spi.v2.model.DisplayPropertyToken;
import com.cloudera.director.spi.v2.model.util.SimpleDisplayPropertyBuilder;
import com.cloudera.director.spi.v2.util.DisplayPropertiesUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EC2 compute instance.
 */
public class EC2Instance extends AbstractComputeInstance<EC2InstanceTemplate, Instance> {

  /**
   * The list of display properties (including inherited properties).
   */
  private static final List<DisplayProperty> DISPLAY_PROPERTIES =
      DisplayPropertiesUtil.asDisplayPropertyList(EC2InstanceDisplayPropertyToken.values());

  /**
   * Returns the list of display properties for an EC2 instance, including inherited properties.
   *
   * @return the list of display properties for an EC2 instance, including inherited properties
   */
  public static List<DisplayProperty> getDisplayProperties() {
    return DISPLAY_PROPERTIES;
  }

  /**
   * EC2 compute instance display properties.
   */
  public enum EC2InstanceDisplayPropertyToken implements DisplayPropertyToken {

    /**
     * The architecture of the image.
     */
    ARCHITECTURE(new SimpleDisplayPropertyBuilder()
        .displayKey("architecture")
        .name("Architecture")
        .defaultDescription("The architecture of the image.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getArchitecture();
      }
    },

    /**
     * Whether the instance is optimized for EBS I/O.
     */
    EBS_OPTIMIZED(new SimpleDisplayPropertyBuilder()
        .displayKey("ebsOptimized")
        .name("EBS-optimized")
        .defaultDescription("Whether the instance is optimized for EBS I/O.")
        .widget(DisplayProperty.Widget.CHECKBOX)
        .type(DisplayProperty.Type.BOOLEAN)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getEbsOptimized().toString();
      }
    },

    /**
     * The hypervisor type of the instance.
     */
    HYPERVISOR(new SimpleDisplayPropertyBuilder()
        .displayKey("hypervisor")
        .name("Hypervisor")
        .defaultDescription("The hypervisor type of the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getHypervisor();
      }
    },

    /**
     * The ID of the AMI used to launch the instance.
     */
    IMAGE_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("imageId")
        .name("Image (AMI) ID")
        .defaultDescription("The ID of the AMI used to launch the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getImageId();
      }
    },

    /**
     * The ID of the instance.
     */
    INSTANCE_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("instanceId")
        .name("Instance ID")
        .defaultDescription("The ID of the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getInstanceId();
      }
    },

    /**
     * The instance lifecycle (e.g. spot).
     */
    INSTANCE_LIFECYCLE(new SimpleDisplayPropertyBuilder()
        .displayKey("instanceLifecycle")
        .name("Lifecycle")
        .defaultDescription("The instance lifecycle (e.g. spot).")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getInstanceLifecycle();
      }
    },

    /**
     * The instance type.
     */
    INSTANCE_TYPE(new SimpleDisplayPropertyBuilder()
        .displayKey("instanceType")
        .name("Instance type")
        .defaultDescription("The instance type.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getInstanceType();
      }
    },

    /**
     * The name of the key pair, if this instance was launched with an associated key pair.
     */
    KEY_PAIR(new SimpleDisplayPropertyBuilder()
        .displayKey("keyName")
        .name("Key pair name")
        .defaultDescription("The name of the key pair, if this instance was launched with an associated key pair.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getKeyName();
      }
    },

    /**
     * The time the instance was launched.
     */
    LAUNCH_TIME(new SimpleDisplayPropertyBuilder()
        .displayKey("launchTime")
        .name("Launch time")
        .defaultDescription("The time the instance was launched.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        // TODO use appropriate date formatting
        return instance.getLaunchTime().toString();
      }
    },

    /**
     * The platform of the instance (Windows for Windows instances; otherwise blank).
     */
    PLATFORM(new SimpleDisplayPropertyBuilder()
        .displayKey("platform")
        .name("Platform")
        .defaultDescription("The platform of the instance (Windows for Windows instances; otherwise blank).")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPlatform();
      }
    },

    /**
     * The private DNS name assigned to the instance.
     */
    PRIVATE_DNS_NAME(new SimpleDisplayPropertyBuilder()
        .displayKey("privateDnsName")
        .name("Private DNS")
        .defaultDescription("The private DNS name assigned to the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPrivateDnsName();
      }
    },

    /**
     * The private IP address assigned to the instance.
     */
    PRIVATE_IP_ADDRESS(new SimpleDisplayPropertyBuilder()
        .displayKey("privateIpAddress")
        .name("Private IP")
        .defaultDescription("The private IP address assigned to the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPrivateIpAddress();
      }
    },

    /**
     * The public DNS name assigned to the instance.
     */
    PUBLIC_DNS_NAME(new SimpleDisplayPropertyBuilder()
        .displayKey("publicDnsName")
        .name("Public DNS")
        .defaultDescription("The public DNS name assigned to the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPublicDnsName();
      }
    },

    /**
     * The public IP address assigned to the instance.
     */
    PUBLIC_IP_ADDRESS(new SimpleDisplayPropertyBuilder()
        .displayKey("publicIpAddress")
        .name("Public IP")
        .defaultDescription("The public IP address assigned to the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPublicIpAddress();
      }
    },

    /**
     * The root device name (for example, /dev/sda1).
     */
    ROOT_DEVICE_NAME(new SimpleDisplayPropertyBuilder()
        .displayKey("rootDeviceName")
        .name("Root device")
        .defaultDescription("The root device name (for example, /dev/sda1).")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getRootDeviceName();
      }
    },

    /**
     * The root device type used by the AMI.
     */
    ROOT_DEVICE_TYPE(new SimpleDisplayPropertyBuilder()
        .displayKey("rootDeviceType")
        .name("Root device type")
        .defaultDescription("The root device type used by the AMI.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getRootDeviceType();
      }
    },

    /**
     * Whether to enable an instance launched in a VPC to perform NAT.
     */
    SOURCE_DEST_CHECK(new SimpleDisplayPropertyBuilder()
        .displayKey("sourceDestCheck")
        .name("Source/dest. check")
        .defaultDescription("Whether to enable an instance launched in a VPC to perform NAT.")
        .widget(DisplayProperty.Widget.CHECKBOX)
        .type(DisplayProperty.Type.BOOLEAN)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        Boolean sourceDestCheck = instance.getSourceDestCheck();
        return Boolean.toString(sourceDestCheck != null && sourceDestCheck);
      }
    },

    /**
     * Whether the instance is a Spot Instance.
     */
    SPOT(new SimpleDisplayPropertyBuilder()
        .displayKey("spot")
        .name("Spot Instance")
        .defaultDescription("Whether the instance is a Spot Instance.")
        .widget(DisplayProperty.Widget.CHECKBOX)
        .type(DisplayProperty.Type.BOOLEAN)
        .sensitive(false)
        .build()) {
      @SuppressWarnings("PMD.EmptyCatchBlock")
      @Override
      protected String getPropertyValue(Instance instance) {
        boolean spot = false;
        try {
          InstanceLifecycleType instanceLifecycleType =
              InstanceLifecycleType.fromValue(instance.getInstanceLifecycle());
          spot = InstanceLifecycleType.Spot.equals(instanceLifecycleType);
        } catch (IllegalArgumentException ignore) {
        }
        return String.valueOf(spot);
      }
    },

    /**
     * Whether enhanced networking is enabled.
     */
    SRIOV_NET_SUPPORT(new SimpleDisplayPropertyBuilder()
        .displayKey("sriovNetSupport")
        .name("Enhanced networking (SR-IOV)")
        .defaultDescription("Whether enhanced networking with the " +
            "Intel 82599 Virtual Function interface is enabled.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getSriovNetSupport();
      }
    },

    /**
     * The name and IDs of all the security groups attached to this instance.
     */
    SECURITY_GROUPS(new SimpleDisplayPropertyBuilder()
        .displayKey("securityGroups")
        .name("Security Groups")
        .defaultDescription("Security group names and IDs attached to this instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        List<String> result = Lists.newArrayList();
        for (GroupIdentifier group : instance.getSecurityGroups()) {
          result.add(String.format("%s(%s)", group.getGroupName(), group.getGroupId()));
        }
        return Joiner.on(", ").join(result);
      }
    },

    /**
     * The raw IDs of all the security groups attached to this instance. This is meant to be used when pushing
     * instance properties back into the instance template.
     */
    SECURITY_GROUPS_IDS_RAW(new SimpleDisplayPropertyBuilder()
        .displayKey("securityGroupsIdsRaw")
        .name("Security Groups")
        .defaultDescription("Security group IDs attached to this instance.")
        .sensitive(false)
        .hidden(true)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        // Sorted to avoid false detection of modification
        Set<String> result = Sets.newTreeSet();
        for (GroupIdentifier group : instance.getSecurityGroups()) {
          result.add(group.getGroupId());
        }
        return Joiner.on(",").join(result);
      }
    },


    /**
     * The ID of the subnet in which the instance is running.
     */
    SUBNET_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("subnetId")
        .name("Subnet ID")
        .defaultDescription("The ID of the subnet in which the instance is running.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getSubnetId();
      }
    },

    /**
     * The availability zone in which the instance is running.
     */
    AVAILABILITY_ZONE(new SimpleDisplayPropertyBuilder()
        .displayKey("availabilityZone")
        .name("Availability Zone")
        .defaultDescription("The ID of the availability zone in which the instance is running.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPlacement().getAvailabilityZone();
      }
    },

    /**
     * The name of the placement group used by this instance.
     */
    PLACEMENT_GROUP_NAME(new SimpleDisplayPropertyBuilder()
        .displayKey("placementGroupName")
        .name("Placement Group")
        .defaultDescription("Placement group name in which the instance is running.")
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPlacement().getGroupName();
      }
    },

    /**
     * The tenancy type of the instance.
     */
    TENANCY(new SimpleDisplayPropertyBuilder()
        .displayKey("tenancy")
        .name("Tenancy")
        .defaultDescription("Tenancy of the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPlacement().getTenancy();
      }
    },

    /**
     * The virtualization type of the instance.
     */
    VIRTUALIZATION_TYPE(new SimpleDisplayPropertyBuilder()
        .displayKey("virtualizationType")
        .name("Virtualization")
        .defaultDescription("The virtualization type of the instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getVirtualizationType();
      }
    },

    /**
     * The ID of the VPC in which the instance is running.
     */
    VPC_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("vpcId")
        .name("VPC ID")
        .defaultDescription("The ID of the VPC in which the instance is running.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getVpcId();
      }
    };

    /**
     * The display property.
     */
    private final DisplayProperty displayProperty;

    /**
     * Creates an EC2 instance display property token with the specified parameters.
     *
     * @param displayProperty the display property
     */
    EC2InstanceDisplayPropertyToken(DisplayProperty displayProperty) {
      this.displayProperty = displayProperty;
    }

    /**
     * Returns the value of the property from the specified instance.
     *
     * @param instance the instance
     * @return the value of the property from the specified instance
     */
    protected abstract String getPropertyValue(Instance instance);

    @Override
    public DisplayProperty unwrap() {
      return displayProperty;
    }

  }

  /**
   * EC2 virtualization types.
   *
   * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/virtualization_types.html"/>
   */
  private enum EC2VirtualizationType {

    /**
     * Hardware virtual machine (HVM).
     */
    HVM("hvm", VirtualizationType.HARDWARE_ASSISTED),

    /**
     * Paravirtual (PV).
     */
    PV("paravirtual", VirtualizationType.PARAVIRTUALIZATION);

    /**
     * The map from type strings to EC2 virtualization types.
     */
    private static final Map<String, EC2VirtualizationType> TYPES_BY_KEY =
        ImmutableMap.<String, EC2VirtualizationType>builder()
            .put(HVM.getKey(), HVM)
            .put(PV.getKey(), PV)
            .build();

    /**
     * Returns the EC2 virtualization type identified by the specified key, or <code>null</code>
     * if there is no such EC2 virtualization type.
     *
     * @param key the type string
     * @return the EC2 virtualization type identified by the specified key, or <code>null</code>
     * if there is no such EC2 virtualization type
     */
    private static EC2VirtualizationType forKey(String key) {
      return TYPES_BY_KEY.get(key);
    }

    /**
     * The virtualization type string, as returned by {@code Instance.getVirtualizationType()}.
     */
    private final String key;

    /**
     * The provider-agnostic virtualization type.
     */
    private final VirtualizationType virtualizationType;

    /**
     * Creates an EC2 virtualization type with the specified parameters.
     *
     * @param key                the virtualization type string, as returned by
     *                           {@code Instance.getVirtualizationType()}.
     * @param virtualizationType the provider-agnostic virtualization type
     */
    EC2VirtualizationType(String key, VirtualizationType virtualizationType) {
      this.key = key;
      this.virtualizationType = virtualizationType;
    }

    /**
     * Returns the virtualization type string, as returned by
     * {@code Instance.getVirtualizationType()}.
     *
     * @return the virtualization type string, as returned by
     * {@code Instance.getVirtualizationType()}
     */
    public String getKey() {
      return key;
    }

    /**
     * Returns the provider-agnostic virtualization type.
     *
     * @return the provider-agnostic virtualization type
     */
    public VirtualizationType getVirtualizationType() {
      return virtualizationType;
    }
  }

  /**
   * The resource type representing an EC2 instance.
   */
  public static final Type TYPE = new ResourceType("EC2Instance");

  /**
   * Returns the private IP address of the specified EC2 instance.
   *
   * @param instance the instance
   * @return the private IP address of the specified EC2 instance
   * @throws IllegalArgumentException if the instance does not have a valid private IP address
   */
  public static InetAddress getPrivateIpAddress(Instance instance) {
    Preconditions.checkNotNull(instance, "instance is null");
    InetAddress privateIpAddress = null;
    try {
      String privateIpString = instance.getPrivateIpAddress();
      if (privateIpString != null && !privateIpString.isEmpty()) {
        privateIpAddress = InetAddress.getByName(privateIpString);
      }
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid private IP address", e);
    }
    return privateIpAddress;
  }

  /**
   * Returns the provider-agnostic virtualization type for the specified EC2 instance.
   *
   * @param instance the instance
   * @return the provider-agnostic virtualization type for the specified EC2 instance
   */
  private static VirtualizationType getVirtualizationType(Instance instance) {
    String key = instance.getVirtualizationType();
    EC2VirtualizationType ec2VirtualizationType = EC2VirtualizationType.forKey(key);
    return (ec2VirtualizationType == null)
        ? VirtualizationType.UNKNOWN
        : ec2VirtualizationType.getVirtualizationType();
  }

  /**
   * Creates an EC2 compute instance with the specified parameters.
   *
   * @param template        the template from which the instance was created
   * @param instanceId      the instance identifier
   * @param instanceDetails the provider-specific instance details
   * @throws IllegalArgumentException if the instance does not have a valid private IP address
   */
  public EC2Instance(EC2InstanceTemplate template, String instanceId, Instance instanceDetails) {
    super(template, instanceId, getPrivateIpAddress(instanceDetails),
        getVirtualizationType(instanceDetails), instanceDetails);
  }

  @Override
  public Type getType() {
    return TYPE;
  }

  @Override
  public Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap();
    Instance instance = unwrap();
    if (instance != null) {
      for (EC2InstanceDisplayPropertyToken propertyToken : EC2InstanceDisplayPropertyToken.values()) {
        properties.put(propertyToken.unwrap().getDisplayKey(), propertyToken.getPropertyValue(instance));
      }
    }
    return properties;
  }

  /**
   * Sets the EC2 instance.
   *
   * @param instance the EC2 instance
   * @throws IllegalArgumentException if the instance does not have a valid private IP address
   */
  protected void setInstance(Instance instance) {
    super.setDetails(instance);
    InetAddress privateIpAddress = getPrivateIpAddress(instance);
    setPrivateIpAddress(privateIpAddress);
    VirtualizationType virtualizationType = getVirtualizationType(instance);
    setVirtualizationType(virtualizationType);
  }
}
