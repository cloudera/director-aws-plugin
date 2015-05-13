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

import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ALLOCATED_STORAGE;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.AUTO_MINOR_VERSION_UPGRADE;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.AVAILABILITY_ZONE;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.BACKUP_RETENTION_PERIOD;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.CHARACTER_SET_NAME;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_INSTANCE_IDENTIFIER;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_NAME;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_PARAMETER_GROUP_NAME;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_SECURITY_GROUP_IDS;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_SUBNET_GROUP_NAME;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ENGINE;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ENGINE_VERSION;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.INSTANCE_CLASS;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.IOPS;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.KMS_KEY_ID;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.LICENSE_MODEL;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.MASTER_USERNAME;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.MASTER_USER_PASSWORD;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.OPTION_GROUP_NAME;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.PORT;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.PREFERRED_BACKUP_WINDOW;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.PREFERRED_MAINTENANCE_WINDOW;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.STORAGE_ENCRYPTED;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.STORAGE_TYPE;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.TDE_CREDENTIAL_ARN;
import static com.cloudera.director.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.VPC_SECURITY_GROUP_IDS;

import com.cloudera.director.spi.v1.database.DatabaseServerInstanceTemplate;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.ResourceTemplate;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;

import java.util.List;
import java.util.Map;

/**
 * Represents a template for constructing RDS database server instances.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class RDSInstanceTemplate extends DatabaseServerInstanceTemplate {

  /**
   * A splitter for comma-separated lists.
   */
  private static final Splitter CSV_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  /**
   * The list of configuration properties (including inherited properties).
   */
  private static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
      ConfigurationPropertiesUtil.merge(
          DatabaseServerInstanceTemplate.getConfigurationProperties(),
          ConfigurationPropertiesUtil.asConfigurationPropertyList(
              RDSInstanceTemplateConfigurationPropertyToken.values())
      );

  /**
   * Returns the list of configuration properties for creating an RDS instance template,
   * including inherited properties.
   *
   * @return the list of configuration properties for creating an RDS instance template,
   * including inherited properties
   */
  public static List<ConfigurationProperty> getConfigurationProperties() {
    return CONFIGURATION_PROPERTIES;
  }

  /**
   * RDS compute instance configuration properties.
   */
  // Fully qualifying class name due to compiler bug
  public static enum RDSInstanceTemplateConfigurationPropertyToken
      implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

    /**
     * The amount of storage (in gigabytes) to be initially allocated for the database instance.
     */
    ALLOCATED_STORAGE(new SimpleConfigurationPropertyBuilder()
        .configKey("allocatedStorage")
        .name("Allocated storage (GB)")
        .required(true)
        .defaultDescription(
            "The amount of storage (in gigabytes) to be initially allocated"
                + " for the database instance.")
        .defaultErrorMessage("Allocated storage is mandatory")
        .build()),

    /**
     * Indicates that minor engine upgrades will be applied automatically to the DB instance
     * during the maintenance window.
     */
    AUTO_MINOR_VERSION_UPGRADE(new SimpleConfigurationPropertyBuilder()
        .configKey("autoMinorVersionUpgrade")
        .name("Auto minor version upgrade")
        .widget(ConfigurationProperty.Widget.CHECKBOX)
        .defaultValue("true")
        .defaultDescription(
            "Indicates that minor engine upgrades will be applied automatically to the DB instance"
                + " during the maintenance window.")
        .build()),

    /**
     * <p>The availability zone.</p>
     * <p/>
     * <p>Multiple availability zones are linked together by high speed low latency connections.
     * Each zone is a distinct failure domain.</p>
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html" />
     */
    AVAILABILITY_ZONE(new SimpleConfigurationPropertyBuilder()
        .configKey("availabilityZone")
        .name("EC2 availability zone")
        .defaultDescription(
            "The EC2 Availability Zone in which the database instance will be created.")
        .hidden(true)
        .build()),

    /**
     * The number of days for which automated backups are retained.
     */
    BACKUP_RETENTION_PERIOD(new SimpleConfigurationPropertyBuilder()
        .configKey("backupRetentionPeriod")
        .name("Backup retention period (days)")
        .defaultDescription("The number of days for which automated backups are retained.")
        .build()),

    /**
     * For supported engines, indicates that the DB instance should be associated
     * with the specified CharacterSet.
     */
    CHARACTER_SET_NAME(new SimpleConfigurationPropertyBuilder()
        .configKey("characterSetName")
        .name("Character set name")
        .defaultDescription(
            "For supported engines, indicates that the DB instance should be associated"
                + " with the specified CharacterSet.")
        .build()),

    /**
     * The instance class, which represents the compute and memory capacity of the DB instance.
     */
    INSTANCE_CLASS(new SimpleConfigurationPropertyBuilder()
        .configKey("instanceClass")
        .name("Instance class")
        .required(true)
        .defaultDescription(
            "The instance class, which represents the compute and memory capacity"
                + " of the DB instance.")
        .defaultErrorMessage("Instance class is mandatory")
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .addValidValues(
            "db.t1.micro",
            "db.m1.small",
            "db.m3.medium",
            "db.m3.large",
            "db.m3.xlarge",
            "db.m3.2xlarge",
            "db.r3.large",
            "db.r3.xlarge",
            "db.r3.2xlarge",
            "db.r3.4xlarge",
            "db.r3.8xlarge",
            "db.t2.micro",
            "db.t2.small",
            "db.t2.medium",
            "db.m2.xlarge",
            "db.m2.2xlarge",
            "db.m2.4xlarge",
            "db.cr1.8xlarge",
            "db.m1.medium",
            "db.m1.large",
            "db.m1.xlarge"
        )
        .build()),

    /**
     * The DB instance identifier.
     */
    DB_INSTANCE_IDENTIFIER(new SimpleConfigurationPropertyBuilder()
        .configKey("dbInstanceIdentifier")
        .name("DB instance identifier")
        .defaultDescription("The DB instance identifier.")
        .build()),

    /**
     * The database name (meaning varies by database engine).
     */
    DB_NAME(new SimpleConfigurationPropertyBuilder()
        .configKey("dbName")
        .name("DB name")
        .defaultDescription("The database name (meaning varies by database engine).")
        .build()),

    /**
     * The name of the DB parameter group to associate with this DB instance.
     */
    DB_PARAMETER_GROUP_NAME(new SimpleConfigurationPropertyBuilder()
        .configKey("dbParameterGroupName")
        .name("DB parameter group name")
        .defaultDescription(
            "The name of the DB parameter group to associate with this DB instance.")
        .build()),

    /**
     * The comma-separated list of DB security groups to associate with this DB instance.
     */
    DB_SECURITY_GROUP_IDS(new SimpleConfigurationPropertyBuilder()
        .configKey("dbSecurityGroups")
        .name("DB security groups")
        .defaultDescription(
            "The comma-separated list of DB security groups to associate with this DB instance.")
        .build()),

    /**
     * The DB subnet group to associate with this DB instance.
     */
    DB_SUBNET_GROUP_NAME(new SimpleConfigurationPropertyBuilder()
        .configKey("dbSubnetGroupName")
        .name("DB subnet group name")
        .required(true)
        .defaultDescription("The DB subnet group to associate with this DB instance.")
        .defaultErrorMessage("DB subnet group name is mandatory")
        .build()),

    /**
     * The name of the database engine to be used for this instance.
     */
    ENGINE(new SimpleConfigurationPropertyBuilder()
        .configKey("engine")
        .name("DB engine")
        .defaultDescription("The name of the database engine to be used for this instance.")
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .addValidValues(
            "mysql"
            //"oracle-ee",
            //"oracle-se",
            //"oracle-se1",
            //"postgres",
            //"sqlserver-ee",
            //"sqlserver-ex",
            //"sqlserver-se",
            //"sqlserver-web"
        )
        .build()),

    /**
     * The version number of the database engine to use.
     */
    ENGINE_VERSION(new SimpleConfigurationPropertyBuilder()
        .configKey("engineVersion")
        .name("DB engine version")
        .defaultDescription("The version number of the database engine to use.")
        .build()),

    /**
     * The amount of Provisioned IOPS (input/output operations per second)
     * to be initially allocated for the DB instance.
     */
    IOPS(new SimpleConfigurationPropertyBuilder()
        .configKey("iops")
        .name("IOPS")
        .defaultDescription(
            "The amount of Provisioned IOPS (input/output operations per second)"
                + " to be initially allocated for the DB instance.")
        .build()),

    /**
     * The KMS key identifier for an encrypted DB instance.
     */
    KMS_KEY_ID(new SimpleConfigurationPropertyBuilder()
        .configKey("kmsKeyId")
        .name("KMS key ID")
        .defaultDescription("The KMS key identifier for an encrypted DB instance.")
        .build()),

    /**
     * The license model information for this DB instance.
     */
    LICENSE_MODEL(new SimpleConfigurationPropertyBuilder()
        .configKey("licenseModel")
        .name("License model")
        .defaultDescription("The license model information for this DB instance.")
        .build()),

    /**
     * The name of master user for the client DB instance.
     */
    MASTER_USERNAME(new SimpleConfigurationPropertyBuilder()
        .configKey("masterUsername")
        .name("Master username")
        .defaultDescription("The name of master user for the client DB instance.")
        .build()),

    /**
     * The password for the master database user.
     */
    MASTER_USER_PASSWORD(new SimpleConfigurationPropertyBuilder()
        .configKey("masterUserPassword")
        .name("Master user password")
        .defaultDescription("The password for the master database user.")
        .build()),

    /**
     * Whether the DB instance is a Multi-AZ deployment.
     */
    MULTI_AZ(new SimpleConfigurationPropertyBuilder()
        .configKey("multiAZ")
        .name("Multi AZ")
        .defaultDescription("Whether the DB instance is a Multi-AZ deployment.")
        .build()),

    /**
     * The option group with which the DB instance should be associated.
     */
    OPTION_GROUP_NAME(new SimpleConfigurationPropertyBuilder()
        .configKey("optionGroupName")
        .name("Option group name")
        .defaultDescription("The option group with which the DB instance should be associated.")
        .build()),

    /**
     * The port number on which the database accepts connections.
     */
    PORT(new SimpleConfigurationPropertyBuilder()
        .configKey("port")
        .name("Port")
        .defaultDescription("The port number on which the database accepts connections.")
        .build()),

    /**
     * The daily time range during which automated backups are created
     * if automated backups are enabled, using the BackupRetentionPeriod parameter.
     */
    PREFERRED_BACKUP_WINDOW(new SimpleConfigurationPropertyBuilder()
        .configKey("preferredBackupWindow")
        .name("Preferred backup window")
        .defaultDescription(
            "The daily time range during which automated backups are created"
                + " if automated backups are enabled, using the BackupRetentionPeriod parameter.")
        .build()),

    /**
     * The weekly time range (in UTC) during which system maintenance can occur.
     */
    PREFERRED_MAINTENANCE_WINDOW(new SimpleConfigurationPropertyBuilder()
        .configKey("preferredMaintenanceWindow")
        .name("Preferred maintenance window")
        .defaultDescription(
            "The weekly time range (in UTC) during which system maintenance can occur.")
        .build()),

    /**
     * Whether the DB instance is publicly accessible.
     */
    PUBLICLY_ACCESSIBLE(new SimpleConfigurationPropertyBuilder()
        .configKey("publiclyAccessible")
        .name("Publicly accessible")
        .defaultDescription("Whether the DB instance is publicly accessible")
        .build()),

    /**
     * Whether the DB instance is encrypted.
     */
    STORAGE_ENCRYPTED(new SimpleConfigurationPropertyBuilder()
        .configKey("storageEncrypted")
        .name("Storage encrypted")
        .defaultDescription("Whether the DB instance is encrypted.")
        .build()),

    /**
     * The storage type to be associated with the DB instance.
     */
    STORAGE_TYPE(new SimpleConfigurationPropertyBuilder()
        .configKey("storageType")
        .name("Storage type")
        .defaultDescription("The storage type to be associated with the DB instance.")
        .build()),

    /**
     * The ARN from the Key Store with which to associate the instance for TDE encryption.
     */
    TDE_CREDENTIAL_ARN(new SimpleConfigurationPropertyBuilder()
        .configKey("tdeCredentialArn")
        .name("TDE credential ARN")
        .defaultDescription(
            "The ARN from the Key Store with which to associate the instance for TDE encryption.")
        .build()),

    /**
     * The comma-separated list of EC2 VPC security groups to associate with this DB instance.
     */
    VPC_SECURITY_GROUP_IDS(new SimpleConfigurationPropertyBuilder()
        .configKey("vpcSecurityGroupIds")
        .name("VPC security group IDs")
        .required(true)
        .defaultDescription(
            "The comma-separated list of EC2 VPC security groups"
                + " to associate with this DB instance.")
        .defaultErrorMessage("VPC security group IDs are mandatory")
        .build());

    /**
     * The configuration property.
     */
    private ConfigurationProperty configurationProperty;

    /**
     * Creates a configuration property token with the specified parameters.
     *
     * @param configurationProperty the configuration property
     */
    private RDSInstanceTemplateConfigurationPropertyToken(
        ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    @Override
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }
  }

  /**
   * The amount of storage (in gigabytes) to be initially allocated for the database instance.
   */
  private final int allocatedStorage;

  /**
   * Whether minor engine upgrades will be applied automatically to the DB instance during the
   * maintenance window.
   */
  private final Optional<Boolean> autoMinorVersionUpgrade;

  /**
   * The availability zone.
   */
  private final Optional<String> availabilityZone;

  /**
   * The number of days for which automated backups are retained.
   */
  private final Optional<Integer> backupRetentionPeriod;

  /**
   * The database character set name.
   */
  private final Optional<String> characterSetName;

  /**
   * The instance class, which represents the compute and memory capacity of the DB instance.
   */
  private final String instanceClass;

  /**
   * The DB instance identifier.
   */
  private final Optional<String> dbInstanceIdentifier;

  /**
   * The database name (meaning varies by database engine).
   */
  private final Optional<String> dbName;

  /**
   * The name of the DB parameter group to associate with this DB instance.
   */
  private final Optional<String> dbParameterGroupName;

  /**
   * The list of DB security groups to associate with this DB instance.
   */
  private final Optional<List<String>> dbSecurityGroups;

  /**
   * The DB subnet group to associate with this DB instance.
   */
  private final String dbSubnetGroupName;

  /**
   * The name of the database engine to be used for this instance.
   */
  private final Optional<String> engine;

  /**
   * The version number of the database engine to use.
   */
  private final Optional<String> engineVersion;

  /**
   * The amount of Provisioned IOPS (input/output operations per second) to be initially allocated
   * for the DB instance.
   */
  private final Optional<Integer> iops;

  /**
   * The KMS key identifier for an encrypted DB instance.
   */
  private final Optional<String> kmsKeyId;

  /**
   * The license model information for this DB instance.
   */
  private final Optional<String> licenseModel;

  /**
   * The name of master user for the client DB instance.
   */
  private final Optional<String> masterUsername;

  /**
   * The password for the master database user.
   */
  private final Optional<String> masterUserPassword;

  /**
   * Whether the DB instance is a Multi-AZ deployment.
   */
  private final Optional<Boolean> multiAZ;

  /**
   * The option group with which the DB instance should be associated.
   */
  private final Optional<String> optionGroupName;

  /**
   * The port number on which the database accepts connections.
   */
  private final Optional<Integer> port;

  /**
   * The daily time range during which automated backups are created if automated backups are
   * enabled, using the BackupRetentionPeriod parameter.
   */
  private final Optional<String> preferredBackupWindow;

  /**
   * The weekly time range (in UTC) during which system maintenance can occur.
   */
  private final Optional<String> preferredMaintenanceWindow;

  /**
   * Whether the DB instance is publicly accessible.
   */
  private final Optional<Boolean> publiclyAccessible;

  /**
   * Whether the DB instance is encrypted.
   */
  private final Optional<Boolean> storageEncrypted;

  /**
   * Specifies the storage type to be associated with the DB instance.
   */
  private final Optional<String> storageType;

  /**
   * The ARN from the Key Store with which to associate the instance for TDE encryption.
   */
  private final Optional<String> tdeCredentialArn;

  /**
   * A comma-separated list of EC2 VPC security groups to associate with this DB instance.
   */
  private final List<String> vpcSecurityGroupIds;

  /**
   * Creates an RDS instance template with the specified parameters.
   *
   * @param resourceTemplate    another resource template
   * @param localizationContext the localization context
   */
  public RDSInstanceTemplate(ResourceTemplate resourceTemplate,
      LocalizationContext localizationContext) {
    this(resourceTemplate.getName(), resourceTemplate,
        resourceTemplate.getTags(), localizationContext);
  }

  /**
   * Creates an RDS instance template with the specified parameters.
   *
   * @param name                        the name of the template
   * @param configuration               the source of configuration
   * @param tags                        the map of tags to be applied to resources created from
   *                                    the template
   * @param providerLocalizationContext the parent provider localization context
   */
  public RDSInstanceTemplate(String name, Configured configuration, Map<String, String> tags,
      LocalizationContext providerLocalizationContext) {
    super(name, configuration, tags, providerLocalizationContext);
    LocalizationContext localizationContext = getLocalizationContext();

    int allocatedStorage =
        Integer.parseInt(getConfigurationValue(ALLOCATED_STORAGE, localizationContext));
    if (allocatedStorage <= 0) {
      throw new IllegalArgumentException(
          "Allocated storage must be a positive number of gigabytes.");
    }
    this.allocatedStorage = allocatedStorage;
    this.autoMinorVersionUpgrade =
        getOptionalBooleanConfigurationValue(AUTO_MINOR_VERSION_UPGRADE, localizationContext);
    this.availabilityZone =
        getOptionalConfigurationValue(AVAILABILITY_ZONE, localizationContext);
    this.backupRetentionPeriod =
        getOptionalIntegerConfigurationParameterValue(BACKUP_RETENTION_PERIOD, localizationContext);
    this.characterSetName =
        getOptionalConfigurationValue(CHARACTER_SET_NAME, localizationContext);
    this.instanceClass =
        getConfigurationValue(INSTANCE_CLASS, localizationContext);
    this.dbInstanceIdentifier =
        getOptionalConfigurationValue(DB_INSTANCE_IDENTIFIER, localizationContext);
    this.dbName =
        getOptionalConfigurationValue(DB_NAME, localizationContext);
    this.dbParameterGroupName =
        getOptionalConfigurationValue(DB_PARAMETER_GROUP_NAME, localizationContext);
    this.dbSecurityGroups =
        getOptionalListConfigurationParameterValue(DB_SECURITY_GROUP_IDS, localizationContext);
    this.dbSubnetGroupName =
        getConfigurationValue(DB_SUBNET_GROUP_NAME, localizationContext);
    this.engine =
        getOptionalConfigurationValue(ENGINE, localizationContext);
    this.engineVersion =
        getOptionalConfigurationValue(ENGINE_VERSION, localizationContext);
    this.iops =
        getOptionalIntegerConfigurationParameterValue(IOPS, localizationContext);
    this.kmsKeyId =
        getOptionalConfigurationValue(KMS_KEY_ID, localizationContext);
    this.licenseModel =
        getOptionalConfigurationValue(LICENSE_MODEL, localizationContext);
    this.masterUsername =
        getOptionalConfigurationValue(MASTER_USERNAME, localizationContext);
    this.masterUserPassword =
        getOptionalConfigurationValue(MASTER_USER_PASSWORD, localizationContext);
    this.multiAZ =
        getOptionalBooleanConfigurationValue(AUTO_MINOR_VERSION_UPGRADE, localizationContext);
    this.optionGroupName =
        getOptionalConfigurationValue(OPTION_GROUP_NAME, localizationContext);
    this.port =
        getOptionalIntegerConfigurationParameterValue(PORT, localizationContext);
    this.preferredBackupWindow =
        getOptionalConfigurationValue(PREFERRED_BACKUP_WINDOW, localizationContext);
    this.preferredMaintenanceWindow =
        getOptionalConfigurationValue(PREFERRED_MAINTENANCE_WINDOW, localizationContext);
    this.publiclyAccessible =
        getOptionalBooleanConfigurationValue(AUTO_MINOR_VERSION_UPGRADE, localizationContext);
    this.storageEncrypted =
        getOptionalBooleanConfigurationValue(STORAGE_ENCRYPTED, localizationContext);
    this.storageType =
        getOptionalConfigurationValue(STORAGE_TYPE, localizationContext);
    this.tdeCredentialArn =
        getOptionalConfigurationValue(TDE_CREDENTIAL_ARN, localizationContext);
    this.vpcSecurityGroupIds = CSV_SPLITTER.splitToList(
        getConfigurationValue(VPC_SECURITY_GROUP_IDS, localizationContext));
  }

  /**
   * Returns the optional amount of storage (in gigabytes) to be initially allocated for the
   * database instance.
   *
   * @return the optional amount of storage (in gigabytes) to be initially allocated for the
   * database instance.
   */
  public int getAllocatedStorage() {
    return allocatedStorage;
  }

  /**
   * Returns whether minor engine upgrades will be applied automatically to the DB instance during
   * the maintenance window.
   *
   * @return whether minor engine upgrades will be applied automatically to the DB instance during
   * the maintenance window.
   */
  public Optional<Boolean> getAutoMinorVersionUpgrade() {
    return autoMinorVersionUpgrade;
  }

  /**
   * Returns the optional availability zone.
   *
   * @return the optional availability zone.
   */
  public Optional<String> getAvailabilityZone() {
    return availabilityZone;
  }

  /**
   * Returns the optional number of days for which automated backups are retained.
   *
   * @return the optional number of days for which automated backups are retained.
   */
  public Optional<Integer> getBackupRetentionPeriod() {
    return backupRetentionPeriod;
  }

  /**
   * Returns the optional database character set name.
   *
   * @return the optional database character set name.
   */
  public Optional<String> getCharacterSetName() {
    return characterSetName;
  }

  /**
   * Returns the optional instance class, which represents the compute and memory capacity of the
   * DB instance.
   *
   * @return the optional instance class, which represents the compute and memory capacity of the
   * DB instance.
   */
  public String getInstanceClass() {
    return instanceClass;
  }

  /**
   * Returns the optional DB instance identifier.
   *
   * @return the optional DB instance identifier.
   */
  public Optional<String> getDbInstanceIdentifier() {
    return dbInstanceIdentifier;
  }

  /**
   * Returns the optional database name (meaning varies by database engine).
   *
   * @return the optional database name (meaning varies by database engine).
   */
  public Optional<String> getDbName() {
    return dbName;
  }

  /**
   * Returns the optional name of the DB parameter group to associate with this DB instance.
   *
   * @return the optional name of the DB parameter group to associate with this DB instance.
   */
  public Optional<String> getDbParameterGroupName() {
    return dbParameterGroupName;
  }

  /**
   * Returns the list of DB security groups to associate with this DB instance.
   *
   * @return the list of DB security groups to associate with this DB instance.
   */
  public Optional<List<String>> getDbSecurityGroups() {
    return dbSecurityGroups;
  }

  /**
   * Returns the optional DB subnet group to associate with this DB instance.
   *
   * @return the optional DB subnet group to associate with this DB instance.
   */
  public String getDbSubnetGroupName() {
    return dbSubnetGroupName;
  }

  /**
   * Returns the optional name of the database engine to be used for this instance.
   *
   * @return the optional name of the database engine to be used for this instance.
   */
  public Optional<String> getEngine() {
    return engine;
  }

  /**
   * Returns the optional version number of the database engine to use.
   *
   * @return the optional version number of the database engine to use.
   */
  public Optional<String> getEngineVersion() {
    return engineVersion;
  }

  /**
   * Returns the optional amount of Provisioned IOPS (input/output operations per second) to be
   * initially allocated for the DB instance.
   *
   * @return the optional amount of Provisioned IOPS (input/output operations per second) to be
   * initially allocated for the DB instance.
   */
  public Optional<Integer> getIops() {
    return iops;
  }

  /**
   * Returns the optional KMS key identifier for an encrypted DB instance.
   *
   * @return the optional KMS key identifier for an encrypted DB instance.
   */
  public Optional<String> getKmsKeyId() {
    return kmsKeyId;
  }

  /**
   * Returns the optional license model information for this DB instance.
   *
   * @return the optional license model information for this DB instance.
   */
  public Optional<String> getLicenseModel() {
    return licenseModel;
  }

  /**
   * Returns the optional name of master user for the client DB instance.
   *
   * @return the optional name of master user for the client DB instance.
   */
  public Optional<String> getMasterUsername() {
    return masterUsername;
  }

  /**
   * Returns the optional password for the master database user.
   *
   * @return the optional password for the master database user.
   */
  public Optional<String> getMasterUserPassword() {
    return masterUserPassword;
  }

  /**
   * Returns whether the DB instance is a Multi-AZ deployment.
   *
   * @return whether the DB instance is a Multi-AZ deployment.
   */
  public Optional<Boolean> isMultiAZ() {
    return multiAZ;
  }

  /**
   * Returns the optional option group with which the DB instance should be associated.
   *
   * @return the optional option group with which the DB instance should be associated.
   */
  public Optional<String> getOptionGroupName() {
    return optionGroupName;
  }

  /**
   * Returns the port number on which the database accepts connections.
   *
   * @return the port number on which the database accepts connections.
   */
  public Optional<Integer> getPort() {
    return port;
  }

  /**
   * Returns the optional daily time range during which automated backups are created if
   * automated backups are enabled,
   * using the BackupRetentionPeriod parameter.
   *
   * @return the optional daily time range during which automated backups are created if
   * automated backups are enabled,
   * using the BackupRetentionPeriod parameter.
   */
  public Optional<String> getPreferredBackupWindow() {
    return preferredBackupWindow;
  }

  /**
   * Returns the optional weekly time range (in UTC) during which system maintenance can occur.
   *
   * @return the optional weekly time range (in UTC) during which system maintenance can occur.
   */
  public Optional<String> getPreferredMaintenanceWindow() {
    return preferredMaintenanceWindow;
  }

  /**
   * Returns whether the DB instance is publicly accessible.
   *
   * @return whether the DB instance is publicly accessible.
   */
  public Optional<Boolean> isPubliclyAccessible() {
    return publiclyAccessible;
  }

  /**
   * Returns whether the DB instance is encrypted.
   *
   * @return whether the DB instance is encrypted.
   */
  public Optional<Boolean> isStorageEncrypted() {
    return storageEncrypted;
  }

  /**
   * Returns the optional storage type to be associated with the DB instance.
   *
   * @return the optional storage type to be associated with the DB instance.
   */
  public Optional<String> getStorageType() {
    return storageType;
  }

  /**
   * Returns the optional ARN from the Key Store with which to associate the instance for TDE
   * encryption.
   *
   * @return the optional ARN from the Key Store with which to associate the instance for TDE
   * encryption.
   */
  public Optional<String> getTdeCredentialArn() {
    return tdeCredentialArn;
  }

  /**
   * Returns the comma-separated list of EC2 VPC security groups to associate with this DB instance.
   *
   * @return the comma-separated list of EC2 VPC security groups to associate with this DB instance.
   */
  public List<String> getVpcSecurityGroupIds() {
    return vpcSecurityGroupIds;
  }

  /**
   * Returns the optional integer value of the specified configuration property.
   *
   * @param configurationPropertyToken the configuration property
   * @param localizationContext        the localization context
   * @return the optional integer value of the specified configuration property
   */
  protected Optional<Integer> getOptionalIntegerConfigurationParameterValue(
      com.cloudera.director.spi.v1.model.ConfigurationPropertyToken configurationPropertyToken,
      LocalizationContext localizationContext) {
    String stringConfigurationValue =
        getConfigurationValue(configurationPropertyToken, localizationContext);
    return (stringConfigurationValue == null)
        ? Optional.<Integer>absent()
        : Optional.of(Integer.parseInt(stringConfigurationValue));
  }

  /**
   * Returns the optional boolean value of the specified configuration property.
   *
   * @param configurationPropertyToken the configuration property
   * @param localizationContext        the localization context
   * @return the optional boolean value of the specified configuration property
   */
  protected Optional<Boolean> getOptionalBooleanConfigurationValue(
      com.cloudera.director.spi.v1.model.ConfigurationPropertyToken configurationPropertyToken,
      LocalizationContext localizationContext) {
    String stringConfigurationValue =
        getConfigurationValue(configurationPropertyToken, localizationContext);
    return (stringConfigurationValue == null)
        ? Optional.<Boolean>absent()
        : Optional.of(Boolean.parseBoolean(stringConfigurationValue));
  }

  /**
   * Returns the optional string list value of the specified configuration property.
   *
   * @param configurationPropertyToken the configuration property
   * @param localizationContext        the localization context
   * @return the optional string list value of the specified configuration property
   */
  protected Optional<List<String>> getOptionalListConfigurationParameterValue(
      com.cloudera.director.spi.v1.model.ConfigurationPropertyToken configurationPropertyToken,
      LocalizationContext localizationContext) {
    String stringConfigurationValue =
        getConfigurationValue(configurationPropertyToken, localizationContext);
    return (stringConfigurationValue == null)
        ? Optional.<List<String>>absent()
        : Optional.of(CSV_SPLITTER.splitToList(stringConfigurationValue));
  }

  /**
   * Returns the optional string value of the specified configuration property.
   *
   * @param configurationPropertyToken the configuration property
   * @return the optional string list value of the specified configuration property
   */
  private Optional<String> getOptionalConfigurationValue(
      com.cloudera.director.spi.v1.model.ConfigurationPropertyToken configurationPropertyToken,
      LocalizationContext localizationContext) {
    return Optional.fromNullable(getConfigurationValue(configurationPropertyToken,
        localizationContext));
  }
}
