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

import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ALLOCATED_STORAGE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.AVAILABILITY_ZONE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.DB_SUBNET_GROUP_NAME;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ENGINE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.ENGINE_VERSION;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.INSTANCE_CLASS;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.MASTER_USER_PASSWORD;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.MULTI_AZ;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.STORAGE_ENCRYPTED;
import static com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken.TYPE;
import static com.cloudera.director.spi.v1.model.util.SimpleResourceTemplate.SimpleResourceTemplateConfigurationPropertyToken.NAME;
import static com.cloudera.director.spi.v1.model.util.Validations.addError;
import static com.cloudera.director.spi.v1.util.Preconditions.checkNotNull;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.DBEngineVersion;
import com.amazonaws.services.rds.model.DBInstanceNotFoundException;
import com.amazonaws.services.rds.model.DBSubnetGroupNotFoundException;
import com.amazonaws.services.rds.model.DescribeDBEngineVersionsRequest;
import com.amazonaws.services.rds.model.DescribeDBEngineVersionsResult;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBSubnetGroupsRequest;
import com.cloudera.director.spi.v1.database.DatabaseType;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates RDS instance template configuration.
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "PMD.UnusedPrivateField", "PMD.UnusedPrivateField",
    "unused", "FieldCanBeLocal"})
public class RDSInstanceTemplateConfigurationValidator implements ConfigurationValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDSInstanceTemplateConfigurationValidator.class);

  static final int MINIMUM_ALLOCATED_STORAGE = 5;
  static final int MAXIMUM_ALLOCATED_STORAGE = 3072;
  static final int MINIMUM_MASTER_USER_PASSWORD_LENGTH = 8;

  // RDS error codes
  static final String INVALID_PARAMETER_VALUE = "InvalidParameterValue";

  static final String UNSUPPORTED_TYPE = "Unsupported database type (not one of " +
      Arrays.asList(DatabaseType.values()) + ") : %s";
  static final String INVALID_IDENTIFIER =
      "Invalid name / database identifier (%s): %s";
  static final String INVALID_ENGINE_VERSION = "Invalid engine version: %s";
  static final String INVALID_INSTANCE_CLASS =
      "Invalid RDS instance class, should start with \"db.\": %s";
  static final String INVALID_ALLOCATED_STORAGE_FORMAT_MSG =
      "Allocated storage must be an integer: %s";
  static final String ALLOCATED_STORAGE_TOO_SMALL =
      "Allocated storage too small, must be at least " +
          MINIMUM_ALLOCATED_STORAGE + " GB: %d";
  static final String ALLOCATED_STORAGE_TOO_LARGE =
      "Allocated storage too large, must be at most " +
          MAXIMUM_ALLOCATED_STORAGE + " GB: %d";

  static final String INVALID_IDENTIFIER_REASON_INVALID_CHARACTER =
      "must start with letter and contain only letters, digits, or hyphens";
  static final String INVALID_IDENTIFIER_REASON_ENDS_WITH_HYPHEN =
      "may not end with a hyphen";
  static final String INVALID_IDENTIFIER_REASON_DOUBLE_HYPHEN =
      "may not contain two consecutive hyphens";

  static final String INSTANCE_ALREADY_EXISTS =
      "A database instance with identifier %s already exists";

  static final String DB_SUBNET_GROUP_NOT_FOUND =
      "DB subnet group not found: %s";
  static final String INVALID_DB_SUBNET_GROUP =
      "Invalid DB subnet group name: %s";

  private static final String INVALID_COUNT_EMPTY_MSG = "%s not found: %s";
  private static final String INVALID_COUNT_DUPLICATES_MSG =
      "More than one %s found with identifier %s";

  static final String AVAILABILITY_ZONE_NOT_ALLOWED_FOR_MULTI_AZ =
      "Availability zone must not be set when creating a Multi-AZ deployment: %s";

  static final String ENCRYPTION_NOT_SUPPORTED =
      "Storage encryption is not supported for instance class: %s";

  static final String MASTER_USER_PASSWORD_TOO_SHORT =
      "The master user password has length %d, less than the minimum of " +
      MINIMUM_MASTER_USER_PASSWORD_LENGTH;
  static final String MASTER_USER_PASSWORD_MISSING =
      "The master user password is not specified";

  /**
   * The RDS provider.
   */
  private final RDSProvider provider;

  /**
   * Instance classes that support storage encryption.
   */
  private final RDSEncryptionInstanceClasses encryptionInstanceClasses;

  /**
   * Creates an RDS instance template configuration validator with the specified parameters.
   *
   * @param provider the RDS provider
   * @param encryptionInstanceClasses instance classes that support storage encryption
   */
  public RDSInstanceTemplateConfigurationValidator(RDSProvider provider,
                                                   RDSEncryptionInstanceClasses encryptionInstanceClasses) {
    this.provider = checkNotNull(provider, "provider is null");
    this.encryptionInstanceClasses = checkNotNull(encryptionInstanceClasses,
                                                  "encryptionInstanceClasses is null");
  }

  @Override
  public void validate(String name, Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    AmazonRDSClient client = provider.getClient();

    boolean isValidIdentifier = checkIdentifierFormat(name, accumulator, NAME, localizationContext);
    if (isValidIdentifier) {
      checkIdentifierUniqueness(client, name, NAME, accumulator, localizationContext);
    }
    checkMasterUserPassword(configuration, accumulator, localizationContext);
    checkEngine(client, configuration, accumulator, localizationContext);
    checkInstanceClass(configuration, accumulator, localizationContext);
    checkAllocatedStorage(configuration, accumulator, localizationContext);
    checkDBSubnetGroupName(client, configuration, accumulator, localizationContext);
    checkStorageEncryption(configuration, accumulator, localizationContext);
  }

  // Rules:
  // - start with ASCII letter
  // - contain ASCII letters, digits, hyphens
  // - not end with hyphen
  // - not contain two consecutive hyphens
  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("\\p{Alpha}[\\p{Alnum}-]*");

  /**
   * Validates the specified identifier.
   *
   * @param identifier          the identifier
   * @param accumulator         the exception condition accumulator
   * @param propertyToken       the token representing the configuration property in error
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  boolean checkIdentifierFormat(String identifier,
      PluginExceptionConditionAccumulator accumulator,
      ConfigurationPropertyToken propertyToken,
      LocalizationContext localizationContext) {
    boolean isValidIdentifier = true;

    if (!IDENTIFIER_PATTERN.matcher(identifier).matches()) {
      addError(accumulator, propertyToken, localizationContext,
          null, INVALID_IDENTIFIER, INVALID_IDENTIFIER_REASON_INVALID_CHARACTER, identifier);
      isValidIdentifier = false;
    }
    if (identifier.endsWith("-")) {
      addError(accumulator, propertyToken, localizationContext,
          null, INVALID_IDENTIFIER, INVALID_IDENTIFIER_REASON_ENDS_WITH_HYPHEN, identifier);
      isValidIdentifier = false;
    }
    if (identifier.contains("--")) {
      addError(accumulator, propertyToken, localizationContext,
          null, INVALID_IDENTIFIER, INVALID_IDENTIFIER_REASON_DOUBLE_HYPHEN, identifier);
      isValidIdentifier = false;
    }

    return isValidIdentifier;
  }

  /**
   * Validates that no database instance with the specified identifier already
   * exists.
   *
   * @param client              the RDS client
   * @param identifier          the identifier
   * @param propertyToken       the token representing the configuration property in error
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkIdentifierUniqueness(AmazonRDSClient client, String identifier,
      ConfigurationPropertyToken propertyToken, PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    DescribeDBInstancesRequest request = new DescribeDBInstancesRequest()
        .withDBInstanceIdentifier(identifier);
    try {
      client.describeDBInstances(request);
      addError(accumulator, propertyToken, localizationContext, null, INSTANCE_ALREADY_EXISTS,
          identifier);
    } catch (DBInstanceNotFoundException e) {
      /* Good! */
      LOG.debug("Instance {} does not already exist", identifier);
    } catch (AmazonServiceException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkMasterUserPassword(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String password =
        configuration.getConfigurationValue(MASTER_USER_PASSWORD, localizationContext);
    if (password != null) {
      if (password.length() < MINIMUM_MASTER_USER_PASSWORD_LENGTH) {
        addError(accumulator, MASTER_USER_PASSWORD, localizationContext, null,
                 MASTER_USER_PASSWORD_TOO_SHORT, password.length());
      }
    } else {
      // This should not normally happen
      addError(accumulator, MASTER_USER_PASSWORD, localizationContext, null,
               MASTER_USER_PASSWORD_MISSING);
    }
  }

  /**
   * @param client              the RDS client
   * @param configuration       the configuration to be validated
   * @param accumulator         the exception condition accumulator
   * @param localizationContext the localization context
   */
  @VisibleForTesting
  void checkEngine(AmazonRDSClient client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    // The preceeding validator has already been run, so we know this value is present.
    String type = configuration.getConfigurationValue(TYPE, localizationContext);

    DatabaseType databaseType;
    try {
      // TODO add validation at the superclass level prior to reaching this point.
      databaseType = DatabaseType.valueOf(type);
    } catch (IllegalArgumentException e) {
      addError(accumulator, TYPE, localizationContext,
          null, UNSUPPORTED_TYPE, type);
      return;
    }

    RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken engineErrorToken = TYPE;
    String engine = configuration.getConfigurationValue(ENGINE, localizationContext);
    try {
      if (engine == null) {
        engine = RDSEngine.getDefaultEngine(databaseType).getEngineName();
      } else {
        engineErrorToken = ENGINE;
        Collection<String> engines = RDSEngine.getSupportedEngineNames(databaseType);
        if (!engines.contains(engine)) {
          addError(accumulator, engineErrorToken, localizationContext,
              null, RDSEngine.INVALID_ENGINE, engine);
          return;
        }
      }
    } catch (IllegalArgumentException e) {
      addError(accumulator, engineErrorToken, localizationContext,
          null, e.getMessage());
      return;
    }

    DescribeDBEngineVersionsRequest request =
        new DescribeDBEngineVersionsRequest().withEngine(engine);

    DescribeDBEngineVersionsResult result;
    try {
      result = client.describeDBEngineVersions(request);
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().equals(INVALID_PARAMETER_VALUE)) {
        addError(accumulator, engineErrorToken, localizationContext,
            null, RDSEngine.INVALID_ENGINE, engine);
        return;
      } else {
        throw Throwables.propagate(e);
      }
    }

    String engineVersion = configuration.getConfigurationValue(ENGINE_VERSION, localizationContext);
    if (engineVersion != null) {
      boolean foundVersion = false;
      for (DBEngineVersion dbEngineVersion : result.getDBEngineVersions()) {
        if (engineVersion.equals(dbEngineVersion.getEngineVersion())) {
          foundVersion = true;
          break;
        }
      }
      if (!foundVersion) {
        addError(accumulator, ENGINE_VERSION, localizationContext,
            null, INVALID_ENGINE_VERSION, engineVersion);
      }
    }
  }

  @SuppressWarnings("PMD.CollapsibleIfStatements")
  @VisibleForTesting
  void checkInstanceClass(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String instanceClass =
        configuration.getConfigurationValue(INSTANCE_CLASS, localizationContext);

    // simple sanity check, to catch accidental use of EC2 classes
    if (instanceClass != null && !instanceClass.startsWith("db.")) {
      addError(accumulator, INSTANCE_CLASS, localizationContext,
          null, INVALID_INSTANCE_CLASS, instanceClass);
    }
  }

  @VisibleForTesting
  void checkAllocatedStorage(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String allocatedStorageString =
        configuration.getConfigurationValue(ALLOCATED_STORAGE, localizationContext);

    if (allocatedStorageString != null) {
      try {
        int allocatedStorage = Integer.parseInt(allocatedStorageString);
        if (allocatedStorage < MINIMUM_ALLOCATED_STORAGE) {
          addError(accumulator, ALLOCATED_STORAGE, localizationContext,
              null, ALLOCATED_STORAGE_TOO_SMALL, allocatedStorage);
        } else if (allocatedStorage > MAXIMUM_ALLOCATED_STORAGE) {
          addError(accumulator, ALLOCATED_STORAGE, localizationContext,
              null, ALLOCATED_STORAGE_TOO_LARGE, allocatedStorage);
        }
      } catch (NumberFormatException e) {
        addError(accumulator, ALLOCATED_STORAGE, localizationContext,
            null, INVALID_ALLOCATED_STORAGE_FORMAT_MSG, allocatedStorageString);
      }
    }
  }

  @VisibleForTesting
  void checkDBSubnetGroupName(AmazonRDSClient client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String dbSubnetGroupName =
        configuration.getConfigurationValue(DB_SUBNET_GROUP_NAME, localizationContext);

    DescribeDBSubnetGroupsRequest request =
        new DescribeDBSubnetGroupsRequest().withDBSubnetGroupName(dbSubnetGroupName);
    try {
      client.describeDBSubnetGroups(request);
    } catch (DBSubnetGroupNotFoundException e) {
      addError(accumulator, DB_SUBNET_GROUP_NAME, localizationContext,
          null, DB_SUBNET_GROUP_NOT_FOUND, dbSubnetGroupName);
    } catch (AmazonServiceException e) {
      if (e.getErrorCode().equals(INVALID_PARAMETER_VALUE)) {
        addError(accumulator, DB_SUBNET_GROUP_NAME, localizationContext,
            null, INVALID_DB_SUBNET_GROUP, dbSubnetGroupName);
      } else {
        throw Throwables.propagate(e);
      }
    }
  }

  @VisibleForTesting
  void checkMultiAz(AmazonRDSClient client,
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String multiAzString = configuration.getConfigurationValue(MULTI_AZ, localizationContext);
    if (multiAzString != null) {
      boolean multiAz = Boolean.parseBoolean(multiAzString);

      if (multiAz) {
        // Illegal to set Availability Zone if creating Multi-AZ deployment.
        String availabilityZone =
            configuration.getConfigurationValue(AVAILABILITY_ZONE, localizationContext);
        if (availabilityZone != null) {
          addError(accumulator, AVAILABILITY_ZONE, localizationContext,
              null, AVAILABILITY_ZONE_NOT_ALLOWED_FOR_MULTI_AZ, availabilityZone);
        }
      }
    }
  }

  @VisibleForTesting
  void checkStorageEncryption(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    String storageEncryptedString =
        configuration.getConfigurationValue(STORAGE_ENCRYPTED, localizationContext);

    if (storageEncryptedString != null) {
      boolean storageEncrypted = Boolean.parseBoolean(storageEncryptedString);

      if (storageEncrypted) {
        String instanceClass =
            configuration.getConfigurationValue(INSTANCE_CLASS, localizationContext);
        if (!encryptionInstanceClasses.apply(instanceClass)) {
          addError(accumulator, STORAGE_ENCRYPTED, localizationContext,
              null, ENCRYPTION_NOT_SUPPORTED, instanceClass);
        }
      }
    }
  }
}
