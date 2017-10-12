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

package com.cloudera.director.aws.rds;

import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.Endpoint;
import com.cloudera.director.spi.v2.database.util.AbstractDatabaseServerInstance;
import com.cloudera.director.spi.v2.model.DisplayProperty;
import com.cloudera.director.spi.v2.model.DisplayPropertyToken;
import com.cloudera.director.spi.v2.model.Property;
import com.cloudera.director.spi.v2.model.util.SimpleDisplayProperty;
import com.cloudera.director.spi.v2.model.util.SimpleDisplayPropertyBuilder;
import com.cloudera.director.spi.v2.util.DisplayPropertiesUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * RDS database server instance.
 */
public class RDSInstance extends AbstractDatabaseServerInstance<RDSInstanceTemplate, DBInstance> {

  /**
   * The list of display properties (including inherited properties).
   */
  private static final List<DisplayProperty> DISPLAY_PROPERTIES =
      DisplayPropertiesUtil.asDisplayPropertyList(RDSInstanceDisplayPropertyToken.values());

  /**
   * Returns the list of display properties for an RDS instance, including inherited properties.
   *
   * @return the list of display properties for an RDS instance, including inherited properties
   */
  public static List<DisplayProperty> getDisplayProperties() {
    return DISPLAY_PROPERTIES;
  }

  /**
   * RDS database server instance properties.
   */
  public static enum RDSInstanceDisplayPropertyToken implements DisplayPropertyToken {

    /**
     * The allocated storage size in gigabytes.
     */
    ALLOCATED_STORAGE(new SimpleDisplayPropertyBuilder()
        .displayKey("allocatedStorage")
        .name("Allocated storage (GB)")
        .defaultDescription("The allocated storage size in gigabytes.")
        .sensitive(false)
        .type(Property.Type.INTEGER)
        .widget(DisplayProperty.Widget.TEXT)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getAllocatedStorage());
      }
    },

    /**
     * Whether minor version patches are applied automatically.
     */
    AUTO_MINOR_VERSION_UPGRADE(new SimpleDisplayPropertyBuilder()
        .displayKey("autoMinorVersionUpgrade")
        .name("Auto minor version upgrade")
        .defaultDescription("Whether minor version patches are applied automatically.")
        .widget(DisplayProperty.Widget.CHECKBOX)
        .type(DisplayProperty.Type.BOOLEAN)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getAutoMinorVersionUpgrade());
      }
    },

    /**
     * The name of the Availability Zone the DB instance is located in.
     */
    AVAILABILITY_ZONE(new SimpleDisplayPropertyBuilder()
        .displayKey("availabilityZone")
        .name("Availability zone")
        .defaultDescription("The name of the Availability Zone the DB instance is located in.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getAvailabilityZone();
      }
    },

    /**
     * The number of days for which automatic DB snapshots are retained.
     */
    BACKUP_RETENTION_PERIOD(new SimpleDisplayPropertyBuilder()
        .displayKey("backupRetentionPeriod")
        .name("Backup retention period (days)")
        .defaultDescription("The number of days for which automatic DB snapshots are retained"
            + "; 0 indicates backups are disabled.")
        .type(DisplayProperty.Type.INTEGER)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getBackupRetentionPeriod());
      }
    },

    /**
     * The name of the character set that the instance is associated with.
     */
    CHARACTER_SET_NAME(new SimpleDisplayPropertyBuilder()
        .displayKey("characterSetName")
        .name("Character set")
        .defaultDescription("The name of the character set that the instance is associated with.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getCharacterSetName();
      }
    },

    /**
     * The name of the compute and memory capacity class of the DB instance.
     */
    DB_INSTANCE_CLASS(new SimpleDisplayPropertyBuilder()
        .displayKey("dbInstanceClass")
        .name("Instance class")
        .defaultDescription("The name of the compute and memory capacity class of the DB instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBInstanceClass();
      }
    },

    /**
     * The user-supplied database identifier.
     */
    DB_INSTANCE_IDENTIFIER(new SimpleDisplayPropertyBuilder()
        .displayKey("dbInstanceIdentifier")
        .name("Instance ID")
        .defaultDescription("The user-supplied database identifier.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBInstanceIdentifier();
      }
    },

    /**
     * The current state of the database server.
     */
    DB_INSTANCE_STATUS(new SimpleDisplayPropertyBuilder()
        .displayKey("dbInstanceStatus")
        .name("DB Instance status")
        .defaultDescription("The current state of the database server.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBInstanceStatus();
      }
    },

    /**
     * If StorageEncrypted is true, the region-unique, immutable identifier for the encrypted DB instance.
     */
    DBI_RESOURCE_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("dbiResourceId")
        .name("Encrypted DB Instance ID")
        .defaultDescription("If StorageEncrypted is true, the region-unique, immutable identifier for the encrypted DB instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDbiResourceId();
      }
    },

    DB_NAME(new SimpleDisplayPropertyBuilder()
        .displayKey("dbName")
        .name("DB name")
        .defaultDescription("The DB name (meaning varies by database engine).")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBName();
      }
    },

    ENGINE(new SimpleDisplayPropertyBuilder()
        .displayKey("engine")
        .name("DB engine")
        .defaultDescription("The database engine.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getEngine();
      }
    },

    ENGINE_VERSION(new SimpleDisplayPropertyBuilder()
        .displayKey("engineVersion")
        .name("DB engine version")
        .defaultDescription("The database engine version.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getEngineVersion();
      }
    },

    INSTANCE_CREATE_TIME(new SimpleDisplayPropertyBuilder()
        .displayKey("instanceCreateTime")
        .name("Created time")
        .defaultDescription("The date and time the DB instance was created.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        // TODO use appropriate date formatting
        return String.valueOf(dbInstance.getInstanceCreateTime());
      }
    },

    IOPS(new SimpleDisplayPropertyBuilder()
        .displayKey("iops")
        .name("IOPS")
        .defaultDescription("The Provisioned IOPS (I/O operations per second) value.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getIops());
      }
    },

    /**
     * If StorageEncrypted is true, the KMS key identifier for the encrypted DB instance.
     */
    KMS_KEY_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("kmsKeyId")
        .name("KMS key ID")
        .defaultDescription("If StorageEncrypted is true, the KMS key identifier for the encrypted DB instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getKmsKeyId();
      }
    },

    LATEST_RESTORABLE_TIME(new SimpleDisplayPropertyBuilder()
        .displayKey("latestRestorableTime")
        .name("Latest restore time")
        .defaultDescription("The latest time to which a database can be restored with point-in-time restore.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        // TODO use appropriate date formatting
        return String.valueOf(dbInstance.getLatestRestorableTime());
      }
    },

    LICENSE_MODEL(new SimpleDisplayPropertyBuilder()
        .displayKey("licenseModel")
        .name("License model")
        .defaultDescription("The license model information.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getLicenseModel();
      }
    },

    MASTER_USERNAME(new SimpleDisplayPropertyBuilder()
        .displayKey("masterUsername")
        .name("Master username")
        .defaultDescription("The master username.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getMasterUsername();
      }
    },

    MULTI_AZ(new SimpleDisplayPropertyBuilder()
        .displayKey("multiAZ")
        .name("Multi-AZ")
        .defaultDescription("Whether the DB instance is a Multi-AZ deployment.")
        .widget(DisplayProperty.Widget.CHECKBOX)
        .type(DisplayProperty.Type.BOOLEAN)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getMultiAZ());
      }
    },

    PREFERRED_BACKUP_WINDOW(new SimpleDisplayPropertyBuilder()
        .displayKey("preferredBackupWindow")
        .name("Preferred backup window")
        .defaultDescription("The daily time range during which automated backups are created if automated backups are enabled, as determined by the BackupRetentionPeriod.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getPreferredBackupWindow();
      }
    },

    PREFERRED_MAINTENANCE_WINDOW(new SimpleDisplayPropertyBuilder()
        .displayKey("preferredMaintenanceWindow")
        .name("Preferred maintenance window")
        .defaultDescription("The weekly time range (in UTC) during which system maintenance can occur.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getPreferredMaintenanceWindow();
      }
    },

    PUBLICLY_ACCESSIBLE(new SimpleDisplayPropertyBuilder()
        .displayKey("publiclyAccessible")
        .name("Publicly accessible")
        .defaultDescription("Whether the DB instance is publicly accessible.")
        .widget(DisplayProperty.Widget.CHECKBOX)
        .type(DisplayProperty.Type.BOOLEAN)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getPubliclyAccessible());
      }
    },

    READ_REPLICA_DB_INSTANCE_IDENTIFIERS(new SimpleDisplayPropertyBuilder()
        .displayKey("readReplicaDBInstanceIdentifiers")
        .name("Read replica ID(s)")
        .defaultDescription("Comma separated list of the identifiers of the Read Replicas associated with this DB instance.")
        .widget(DisplayProperty.Widget.MULTI)
        .type(DisplayProperty.Type.STRING)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return Joiner.on(", ").join(dbInstance.getReadReplicaDBInstanceIdentifiers());
      }
    },

    READ_REPLICA_SOURCE_DB_INSTANCE_IDENTIFIER(new SimpleDisplayPropertyBuilder()
        .displayKey("readReplicaSourceDBInstanceIdentifier")
        .name("Read replica source ID")
        .defaultDescription("If the DB instance is a Read Replica, the identifier of the source DB instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getReadReplicaSourceDBInstanceIdentifier();
      }
    },

    SECONDARY_AVAILABILITY_ZONE(new SimpleDisplayPropertyBuilder()
        .displayKey("secondaryAvailabilityZone")
        .name("Secondary availability zone")
        .defaultDescription("The name of the secondary Availability Zone for a DB instance with multi-AZ support.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getSecondaryAvailabilityZone();
      }
    },

    /**
     * Whether the DB instance is encrypted.
     */
    STORAGE_ENCRYPTED(new SimpleDisplayPropertyBuilder()
        .displayKey("storageEncrypted")
        .name("Storage encryption")
        .defaultDescription("Whether the DB instance is encrypted.")
        .widget(DisplayProperty.Widget.CHECKBOX)
        .type(DisplayProperty.Type.BOOLEAN)
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getStorageEncrypted());
      }
    },

    /**
     * The storage type associated with the DB instance.
     */
    STORAGE_TYPE(new SimpleDisplayPropertyBuilder()
        .displayKey("storageType")
        .name("Storage type")
        .defaultDescription("The storage type associated with the DB instance.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getStorageType();
      }
    },

    /**
     * The ARN from the Key Store with which the instance is associated for TDE encryption.
     */
    TDE_CREDENTIAL_ARN(new SimpleDisplayPropertyBuilder()
        .displayKey("tdeCredentialArn")
        .name("TDE encryption ARN")
        .defaultDescription("The Amazon Resource Name (ARN) from the Key Store with which the instance is associated for TDE encryption.")
        .sensitive(false)
        .build()) {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getTdeCredentialArn();
      }
    };

    /**
     * The display property.
     */
    private final DisplayProperty displayProperty;

    /**
     * Creates an RDS instance display property token with the specified parameters.
     *
     * @param displayProperty the display property
     */
    private RDSInstanceDisplayPropertyToken(SimpleDisplayProperty displayProperty) {
      this.displayProperty = displayProperty;
    }

    protected abstract String getPropertyValue(DBInstance instance);

    @Override
    public DisplayProperty unwrap() {
      return displayProperty;
    }
  }

  /**
   * The resource type representing an RDS instance.
   */
  public static final Type TYPE = new ResourceType("RDSInstance");

  /**
   * Returns the private IP address of the specified RDS instance.
   *
   * @param dbInstance the RDS instance
   * @return the private IP address of the specified RDS instance
   */
  private static InetAddress getPrivateIpAddress(DBInstance dbInstance) {
    Preconditions.checkNotNull(dbInstance, "dbInstance is null");
    InetAddress privateIpAddress = null;
    try {
      Endpoint endpoint = dbInstance.getEndpoint();
      if (endpoint != null) {
        String endpointAddress = endpoint.getAddress();
        if (endpointAddress != null) {
          privateIpAddress = InetAddress.getByName(endpointAddress);
        }
      }
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid private IP address", e);
    }
    return privateIpAddress;
  }

  /**
   * Returns the port used for administrative database connections on the specified RDS instance.
   *
   * @param dbInstance the RDS instance
   * @return the port used for administrative database connections on the specified RDS instance
   */
  private static Integer getPort(DBInstance dbInstance) {
    Preconditions.checkNotNull(dbInstance, "dbInstance is null");
    Endpoint endpoint = dbInstance.getEndpoint();
    return (endpoint == null) ? null : endpoint.getPort();
  }

  /**
   * Creates an RDS compute instance with the specified parameters.
   *
   * @param template           the template from which the instance was created
   * @param instanceIdentifier the instance identifier
   * @param instanceDetails    the provider-specific instance details
   */
  protected RDSInstance(RDSInstanceTemplate template, String instanceIdentifier, DBInstance instanceDetails) {
    super(template, instanceIdentifier, getPrivateIpAddress(instanceDetails), getPort(instanceDetails), instanceDetails);
  }

  @Override
  public Type getType() {
    return TYPE;
  }

  @Override
  public Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap();
    DBInstance instance = unwrap();
    if (instance != null) {
      for (RDSInstanceDisplayPropertyToken propertyToken : RDSInstanceDisplayPropertyToken.values()) {
        properties.put(propertyToken.unwrap().getDisplayKey(), propertyToken.getPropertyValue(instance));
      }
    }
    return properties;
  }

  /**
   * Sets the RDS instance.
   *
   * @param dbInstance the RDS instance
   */
  protected void setDBInstance(DBInstance dbInstance) {
    super.setDetails(dbInstance);
    InetAddress privateIpAddress = getPrivateIpAddress(dbInstance);
    setPrivateIpAddress(privateIpAddress);
    Integer port = getPort(dbInstance);
    setPort(port);
  }
}
