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

package com.cloudera.director.ec2;

import static com.cloudera.director.spi.v1.model.CommonLocalizableAttribute.DESCRIPTION;

import com.amazonaws.services.ec2.model.Instance;
import com.cloudera.director.spi.v1.compute.VirtualizationType;
import com.cloudera.director.spi.v1.compute.util.AbstractComputeInstance;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * EC2 compute instance.
 */
public class EC2Instance extends AbstractComputeInstance<EC2InstanceTemplate, Instance> {

  /**
   * EC2 compute instance properties.
   */
  public static enum Property {

    /**
     * The architecture of the image.
     */
    ARCHITECTURE("architecture", false, "The architecture of the image.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getArchitecture();
      }
    },

    /**
     * Whether the instance is optimized for EBS I/O.
     */
    EBS_OPTIMIZED("ebsOptimized", false, "Whether the instance is optimized for EBS I/O.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getEbsOptimized().toString();
      }
    },

    /**
     * The hypervisor type of the instance.
     */
    HYPERVISOR("hypervisor", false, "The hypervisor type of the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getHypervisor();
      }
    },

    /**
     * The ID of the AMI used to launch the instance.
     */
    IMAGE_ID("imageId", false, "The ID of the AMI used to launch the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getImageId();
      }
    },

    /**
     * The ID of the instance.
     */
    INSTANCE_ID("instanceId", false, "The ID of the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getInstanceId();
      }
    },

    /**
     * Whether this is a Spot Instance.
     */
    INSTANCE_LIFECYCLE("instanceLifecycle", false, "Whether this is a Spot Instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getClientToken();
      }
    },

    /**
     * The instance type.
     */
    INSTANCE_TYPE("instanceType", false, "The instance type.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getInstanceType();
      }
    },

    /**
     * The name of the key pair, if this instance was launched with an associated key pair.
     */
    KEY_PAIR("keyName", false, "The name of the key pair, if this instance was launched with an associated key pair.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getKeyName();
      }
    },

    /**
     * The time the instance was launched.
     */
    LAUNCH_TIME("launchTime", false, "The time the instance was launched.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        // TODO use appropriate date formatting
        return instance.getLaunchTime().toString();
      }
    },

    /**
     * The platform of the instance (Windows for Windows instances; otherwise blank).
     */
    PLATFORM("platform", false, "The platform of the instance (Windows for Windows instances; otherwise blank).") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPrivateDnsName();
      }
    },

    /**
     * The private DNS name assigned to the instance.
     */
    PRIVATE_DNS_NAME("privateDnsName", false, "The private DNS name assigned to the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPrivateDnsName();
      }
    },

    /**
     * The private IP address assigned to the instance.
     */
    PRIVATE_IP_ADDRESS("privateIpAddress", false, "The private IP address assigned to the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPrivateIpAddress();
      }
    },

    /**
     * The public DNS name assigned to the instance.
     */
    PUBLIC_DNS_NAME("publicDnsName", false, "The public DNS name assigned to the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPublicDnsName();
      }
    },

    /**
     * The public IP address assigned to the instance.
     */
    PUBLIC_IP_ADDRESS("publicIpAddress", false, "The public IP address assigned to the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getPublicIpAddress();
      }
    },

    /**
     * The root device name (for example, /dev/sda1).
     */
    ROOT_DEVICE_NAME("rootDeviceName", false, "The root device name (for example, /dev/sda1).") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getRootDeviceName();
      }
    },

    /**
     * The root device type used by the AMI.
     */
    ROOT_DEVICE_TYPE("rootDeviceType", false, "The root device type used by the AMI.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getRootDeviceType();
      }
    },

    /**
     * Whether to enable an instance launched in a VPC to perform NAT.
     */
    SOURCE_DEST_CHECK("sourceDestCheck", false, "Whether to enable an instance launched in a VPC to perform NAT.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getSourceDestCheck().toString();
      }
    },

    /**
     * Whether enhanced networking is enabled.
     */
    SRIOV_NET_SUPPORT("sriovNetSupport", false, "Whether enhanced networking is enabled.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getSriovNetSupport();
      }
    },

    /**
     * The ID of the subnet in which the instance is running.
     */
    SUBNET_ID("subnetId", false, "The ID of the subnet in which the instance is running.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getSubnetId();
      }
    },

    /**
     * The virtualization type of the instance.
     */
    VIRTUALIZATION_TYPE("virtualizationType", false, "The virtualization type of the instance.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getVirtualizationType();
      }
    },

    /**
     * The ID of the VPC in which the instance is running.
     */
    VPC_ID("vpcId", false, "The ID of the VPC in which the instance is running.") {
      @Override
      protected String getPropertyValue(Instance instance) {
        return instance.getSourceDestCheck().toString();
      }
    };

    /**
     * The name of the property.
     */
    private final String propertyName;

    /**
     * Whether the property contains sensitive information.
     */
    private final boolean sensitive;

    /**
     * The human-readable description of the property.
     */
    private final String description;

    /**
     * Creates a property with the specified parameters.
     *
     * @param propertyName the name of the property
     * @param sensitive    whether the property should be redacted
     * @param description  the human-readable description of the property
     */
    private Property(String propertyName, boolean sensitive, String description) {
      this.propertyName = propertyName;
      this.sensitive = sensitive;
      this.description = description;
    }

    protected abstract String getPropertyValue(Instance instance);

    /**
     * Returns the name of the property.
     *
     * @return the name of the property
     */
    public String getPropertyName() {
      return propertyName;
    }

    /**
     * Returns whether the property contains sensitive information.
     *
     * @return whether the property contains sensitive information
     */
    public boolean isSensitive() {
      return sensitive;
    }

    /**
     * Returns a human-readable description of the property.
     *
     * @param localizationContext the localization context
     * @return a human-readable description of the property
     */
    public String getDescription(LocalizationContext localizationContext) {
      return (localizationContext == null) ? description
          : localizationContext.localize(description, name(), DESCRIPTION.getKeyComponent());
    }
  }

  /**
   * EC2 virtualization types.
   *
   * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/virtualization_types.html"/>
   */
  private static enum EC2VirtualizationType {

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
    private EC2VirtualizationType(String key, VirtualizationType virtualizationType) {
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
  private static InetAddress getPrivateIpAddress(Instance instance) {
    Preconditions.checkNotNull(instance, "instance is null");
    InetAddress privateIpAddress;
    try {
      privateIpAddress = InetAddress.getByName(instance.getPrivateIpAddress());
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
  protected EC2Instance(EC2InstanceTemplate template, String instanceId, Instance instanceDetails) {
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
      for (Property property : Property.values()) {
        properties.put(property.getPropertyName(), property.getPropertyValue(instance));
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
