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

package com.cloudera.director.rds;

import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.Endpoint;
import com.cloudera.director.spi.v1.database.util.AbstractDatabaseServerInstance;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;

/**
 * RDS database server instance.
 */
public class RDSInstance extends AbstractDatabaseServerInstance<RDSInstanceTemplate, DBInstance> {

  /**
   * RDS database server instance properties.
   */
  public static enum Property {

    /**
     * The allocated storage size in gigabytes.
     */
    ALLOCATED_STORAGE("allocatedStorage", false, "The allocated storage size in gigabytes.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getAllocatedStorage());
      }
    },

    /**
     * Whether minor version patches are applied automatically.
     */
    AUTO_MINOR_VERSION_UPGRADE("autoMinorVersionUpgrade", false, "Whether minor version patches are applied automatically.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getAutoMinorVersionUpgrade());
      }
    },

    /**
     * The name of the Availability Zone the DB instance is located in.
     */
    AVAILABILITY_ZONE("availabilityZone", false, "The name of the Availability Zone the DB instance is located in.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getAvailabilityZone();
      }
    },

    /**
     * The number of days for which automatic DB snapshots are retained.
     */
    BACKUP_RETENTION_PERIOD("backupRetentionPeriod", false, "The number of days for which automatic DB snapshots are retained.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getBackupRetentionPeriod());
      }
    },

    /**
     * The name of the character set that the instance is associated with.
     */
    CHARACTER_SET_NAME("characterSetName", false, "The name of the character set that the instance is associated with.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getCharacterSetName();
      }
    },

    /**
     * The name of the compute and memory capacity class of the DB instance.
     */
    DB_INSTANCE_CLASS("dbInstanceClass", false, "The name of the compute and memory capacity class of the DB instance.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBInstanceClass();
      }
    },

    /**
     * The user-supplied database identifier.
     */
    DB_INSTANCE_IDENTIFIER("dbInstanceIdentifier", false, "The user-supplied database identifier.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBInstanceIdentifier();
      }
    },

    /**
     * The current state of the database server.
     */
    DB_INSTANCE_STATUS("dbInstanceStatus", false, "The current state of the database server.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBInstanceStatus();
      }
    },

    // TODO AWS SDK 1.9+
    // /**
    //  * If StorageEncrypted is true, the region-unique, immutable identifier for the encrypted DB instance.
    //  */
    // DBI_RESOURCE_ID("dbiResourceId", false, "If StorageEncrypted is true, the region-unique, immutable identifier for the encrypted DB instance.") {
    //   @Override
    //   protected String getPropertyValue(DBInstance dbInstance) {
    //     return dbInstance.getDbiResourceId();
    //   }
    // },

    DB_NAME("dbName", false, "The DB name (meaning varies by database engine).") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getDBName();
      }
    },

    ENGINE("engine", false, "The database engine.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getEngine();
      }
    },

    ENGINE_VERSION("engineVersion", false, "The database engine version.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getEngineVersion();
      }
    },

    INSTANCE_CREATE_TIME("instanceCreateTime", false, "The date and time the DB instance was created.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        // TODO use appropriate date formatting
        return String.valueOf(dbInstance.getInstanceCreateTime());
      }
    },

    IOPS("iops", false, "The Provisioned IOPS (I/O operations per second) value.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getIops());
      }
    },

    // TODO AWS SDK 1.9+
    // /**
    //  * If StorageEncrypted is true, the KMS key identifier for the encrypted DB instance.
    //  */
    // KMS_KEY_ID("kmsKeyId", false, "If StorageEncrypted is true, the KMS key identifier for the encrypted DB instance.") {
    //   @Override
    //   protected String getPropertyValue(DBInstance dbInstance) {
    //     return dbInstance.getKmsKeyId();
    //   }
    // },

    LATEST_RESTORABLE_TIME("latestRestorableTime", false, "The latest time to which a database can be restored with point-in-time restore.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        // TODO use appropriate date formatting
        return String.valueOf(dbInstance.getLatestRestorableTime());
      }
    },

    LICENSE_MODEL("licenseModel", false, "The license model information.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getLicenseModel();
      }
    },

    MASTER_USERNAME("masterUsername", false, "The master username.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getMasterUsername();
      }
    },

    MULTI_AZ("multiAZ", false, "Whether the DB instance is a Multi-AZ deployment.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getMultiAZ());
      }
    },

    PREFERRED_BACKUP_WINDOW("preferredBackupWindow", false, "The daily time range during which automated backups are created if automated backups are enabled, as determined by the BackupRetentionPeriod.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getPreferredBackupWindow();
      }
    },

    PREFERRED_MAINTENANCE_WINDOW("preferredMaintenanceWindow", false, "The weekly time range (in UTC) during which system maintenance can occur.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getPreferredMaintenanceWindow();
      }
    },

    PUBLICLY_ACCESSIBLE("publiclyAccessible", false, "Whether the DB instance is publicly accessible.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return String.valueOf(dbInstance.getPubliclyAccessible());
      }
    },

    READ_REPLICA_DB_INSTANCE_IDENTIFIERS("readReplicaDBInstanceIdentifiers", false, "Comma separated list of the identifiers of the Read Replicas associated with this DB instance.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return Joiner.on(", ").join(dbInstance.getReadReplicaDBInstanceIdentifiers());
      }
    },

    READ_REPLICA_SOURCE_DB_INSTANCE_IDENTIFIER("readReplicaSourceDBInstanceIdentifier", false, "If the DB instance is a Read Replica, the identifier of the source DB instance.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getReadReplicaSourceDBInstanceIdentifier();
      }
    },

    SECONDARY_AVAILABILITY_ZONE("secondaryAvailabilityZone", false, "The name of the secondary Availability Zone for a DB instance with multi-AZ support.") {
      @Override
      protected String getPropertyValue(DBInstance dbInstance) {
        return dbInstance.getSecondaryAvailabilityZone();
      }
    };

    // TODO AWS SDK 1.9+
    // /**
    //  * Whether the DB instance is encrypted.
    //  */
    // STORAGE_ENCRYPTED("storageEncrypted", false, "Whether the DB instance is encrypted.") {
    //   @Override
    //   protected String getPropertyValue(DBInstance dbInstance) {
    //     return dbInstance.getStorageEncrypted();
    //   }
    // },

    // TODO AWS SDK 1.9+
    // /**
    //  * The storage type associated with the DB instance.
    //  */
    // STORAGE_TYPE("storageType", false, "The storage type associated with the DB instance.") {
    //   @Override
    //   protected String getPropertyValue(DBInstance dbInstance) {
    //     return dbInstance.getStorageType();
    //   }
    // },

    // TODO AWS SDK 1.9+
    // /**
    //  * The ARN from the Key Store with which the instance is associated for TDE encryption.
    //  */
    // TDE_CREDENTIAL_ARN("tdeCredentialArn", false, "The ARN from the Key Store with which the instance is associated for TDE encryption.") {
    //   @Override
    //   protected String getPropertyValue(DBInstance dbInstance) {
    //     return dbInstance.getTdeCredentialArn();
    //   }
    // };

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

    protected abstract String getPropertyValue(DBInstance instance);

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
     * @return a human-readable description of the property
     */
    public String getDescription(Locale locale) {
      return description;
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
   * @param instanceDetails            the provider-specific instance details
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
      for (Property property : Property.values()) {
        properties.put(property.getPropertyName(), property.getPropertyValue(instance));
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
