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
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.ALLOCATED_STORAGE_TOO_LARGE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.ALLOCATED_STORAGE_TOO_SMALL;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.AVAILABILITY_ZONE_NOT_ALLOWED_FOR_MULTI_AZ;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.DB_SUBNET_GROUP_NOT_FOUND;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.ENCRYPTION_NOT_SUPPORTED;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INSTANCE_ALREADY_EXISTS;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_ALLOCATED_STORAGE_FORMAT_MSG;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_DB_SUBNET_GROUP;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_ENGINE_VERSION;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_IDENTIFIER;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_IDENTIFIER_REASON_DOUBLE_HYPHEN;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_IDENTIFIER_REASON_ENDS_WITH_HYPHEN;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_IDENTIFIER_REASON_INVALID_CHARACTER;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_INSTANCE_CLASS;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.INVALID_PARAMETER_VALUE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.MASTER_USER_PASSWORD_MISSING;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.MASTER_USER_PASSWORD_TOO_SHORT;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.MAXIMUM_ALLOCATED_STORAGE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.MINIMUM_ALLOCATED_STORAGE;
import static com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator.MINIMUM_MASTER_USER_PASSWORD_LENGTH;
import static com.cloudera.director.spi.v1.database.DatabaseType.MYSQL;
import static com.cloudera.director.spi.v1.database.DatabaseType.ORACLE;
import static com.cloudera.director.spi.v1.model.util.SimpleResourceTemplate.SimpleResourceTemplateConfigurationPropertyToken.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.shaded.com.amazonaws.AmazonServiceException;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.AmazonRDSClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DBEngineVersion;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DBInstanceNotFoundException;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DBSubnetGroupNotFoundException;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DescribeDBEngineVersionsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DescribeDBEngineVersionsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DescribeDBSubnetGroupsRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.rds.model.DescribeDBSubnetGroupsResult;
import com.cloudera.director.spi.v1.database.DatabaseType;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({"PMD.TooManyStaticImports", "FieldCanBeLocal"})
public class RDSInstanceTemplateConfigurationValidatorTest {

  private static final String TEST_DB_IDENTIFIER = "mysql1";
  private static final String TEST_DB_SUBNET_GROUP_NAME = "default";
  private static final String TEST_ALLOCATED_STORAGE = "5";
  private static final String TEST_ENGINE_VERSION = "5.5.39b";

  private RDSProvider rdsProvider;
  private RDSEncryptionInstanceClasses rdsEncryptionInstanceClasses;
  private RDSInstanceTemplateConfigurationValidator validator;
  private AmazonRDSClient rdsClient;
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  @Before
  public void setUp() {
    rdsClient = mock(AmazonRDSClient.class);
    rdsProvider = mock(RDSProvider.class);
    when(rdsProvider.getClient()).thenReturn(rdsClient);
    rdsEncryptionInstanceClasses = mock(RDSEncryptionInstanceClasses.class);
    validator = new RDSInstanceTemplateConfigurationValidator(rdsProvider,
                                                              rdsEncryptionInstanceClasses);
    accumulator = new PluginExceptionConditionAccumulator();
  }

  private void mockDescribeDBInstances(String identifier, boolean exists) {
    DescribeDBInstancesRequest request =
        new DescribeDBInstancesRequest().withDBInstanceIdentifier(identifier);
    if (exists) {
      when(rdsClient.describeDBInstances(request))
          .thenReturn(mock(DescribeDBInstancesResult.class));
    } else {
      when(rdsClient.describeDBInstances(request))
          .thenThrow(new DBInstanceNotFoundException(identifier));
    }
  }

  private void mockDescribeDBEngineVersions(String engineVersion,
      boolean valid) {
    for (String engine : RDSEngine.getSupportedEngineNames(MYSQL)) {
      DescribeDBEngineVersionsRequest request =
          new DescribeDBEngineVersionsRequest().withEngine(engine);
      DescribeDBEngineVersionsResult result =
          new DescribeDBEngineVersionsResult().withDBEngineVersions(
              new DBEngineVersion().withEngine(engine).withEngineVersion(engineVersion)
          );
      if (valid) {
        when(rdsClient.describeDBEngineVersions(request)).thenReturn(result);
      } else {
        AmazonServiceException e = new AmazonServiceException("invalid");
        e.setErrorCode(INVALID_PARAMETER_VALUE);
        when(rdsClient.describeDBEngineVersions(request)).thenThrow(e);
      }
    }
  }

  private void mockDescribeDBSubnetGroups(String name, boolean present,
      boolean valid) {
    DescribeDBSubnetGroupsRequest request =
        new DescribeDBSubnetGroupsRequest().withDBSubnetGroupName(name);
    DescribeDBSubnetGroupsResult result =
        new DescribeDBSubnetGroupsResult();
    if (present && valid) {
      when(rdsClient.describeDBSubnetGroups(request)).thenReturn(result);
    } else if (!present) {
      when(rdsClient.describeDBSubnetGroups(request))
          .thenThrow(new DBSubnetGroupNotFoundException(name));
    } else {
      AmazonServiceException e = new AmazonServiceException("invalid");
      e.setErrorCode(INVALID_PARAMETER_VALUE);
      when(rdsClient.describeDBSubnetGroups(request)).thenThrow(e);
    }
  }

  @Test
  public void testGoodIdentifierFormat() {
    checkIdentifierFormat("MySQL-t", NAME);
    verifyClean();
  }

  @Test
  public void testBadIdentifierFormat() {
    String[][] badIdentifiers = {
        new String[]{"1MySQL-t", INVALID_IDENTIFIER_REASON_INVALID_CHARACTER},
        new String[]{"MySQL_t1", INVALID_IDENTIFIER_REASON_INVALID_CHARACTER},
        new String[]{"MySQL-t1-", INVALID_IDENTIFIER_REASON_ENDS_WITH_HYPHEN},
        new String[]{"MySQL--t1", INVALID_IDENTIFIER_REASON_DOUBLE_HYPHEN}
    };

    for (String[] badIdentifier : badIdentifiers) {
      checkIdentifierFormat(badIdentifier[0], NAME);
      verifySingleError(NAME, INVALID_IDENTIFIER, badIdentifier[1], badIdentifier[0]);
      accumulator = new PluginExceptionConditionAccumulator();
    }
  }

  @Test
  public void testCheckIdentifierUniqueness_Absent() {
    mockDescribeDBInstances(TEST_DB_IDENTIFIER, false);
    checkIdentifierUniqueness(TEST_DB_IDENTIFIER, NAME);
    verifyClean();
  }

  @Test
  public void testCheckIdentifierUniqueness_Present() {
    mockDescribeDBInstances(TEST_DB_IDENTIFIER, true);
    checkIdentifierUniqueness(TEST_DB_IDENTIFIER, NAME);
    verifySingleError(NAME, INSTANCE_ALREADY_EXISTS, TEST_DB_IDENTIFIER);
  }

  private static final String PASSWORD_BASE = "Aa1@";
  private static final int PASSWORD_BASE_LEN = PASSWORD_BASE.length();

  @Test
  public void testCheckMasterUserPassword() {
    String password = Strings.repeat(PASSWORD_BASE,
                                     (MINIMUM_MASTER_USER_PASSWORD_LENGTH / PASSWORD_BASE_LEN) + 1);
    checkMasterUserPassword(password);
    verifyClean();
  }

  @Test
  public void testCheckMasterUserPassword_TooShort() {
    String password = PASSWORD_BASE_LEN < MINIMUM_MASTER_USER_PASSWORD_LENGTH ?
        PASSWORD_BASE : PASSWORD_BASE.substring(0, MINIMUM_MASTER_USER_PASSWORD_LENGTH - 2);
    checkMasterUserPassword(password);
    verifySingleError(MASTER_USER_PASSWORD, MASTER_USER_PASSWORD_TOO_SHORT, password.length());
  }

  @Test
  public void testCheckMasterUserPassword_Missing() {
    checkMasterUserPassword(null);
    verifySingleError(MASTER_USER_PASSWORD, MASTER_USER_PASSWORD_MISSING);
  }

  @Test
  public void testCheckEngine() {
    checkEngine(MYSQL.name(), RDSEngine.MYSQL.getEngineName(), null);
    verifyClean();
    checkEngine(MYSQL.name(), RDSEngine.MARIADB.getEngineName(), null);
    verifyClean();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckEngine_MissingDatabaseType() {
    checkEngine(null, RDSEngine.MYSQL.getEngineName(), null);
  }

  @Test
  public void testCheckEngine_InvalidDatabaseType() {
    checkEngine("x", RDSEngine.MYSQL.getEngineName(), null);
    verifySingleError(TYPE);
  }

  @Test
  public void testCheckEngine_UnknownEngine() {
    checkEngine(MYSQL.name(), "x", null);
    verifySingleError(ENGINE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckEngine_UnsupportedEngine() {
    checkEngine(ORACLE, null);
  }

  @Test
  public void testCheckEngine_InvalidEngineDefault() {
    mockDescribeDBEngineVersions(TEST_ENGINE_VERSION, false);
    checkEngine(MYSQL.name(), null, "5.5.40b");
    verifySingleError(TYPE);
  }

  @Test
  public void testCheckEngine_InvalidEngine() {
    mockDescribeDBEngineVersions(TEST_ENGINE_VERSION, false);
    checkEngine(MYSQL.name(), "x", "5.5.40b");
    verifySingleError(ENGINE);
  }

  @Test
  public void testCheckEngineVersion_InvalidEngineVersion() {
    mockDescribeDBEngineVersions(TEST_ENGINE_VERSION, true);
    String engineVersion = "5.5.40b";
    checkEngine(MYSQL.name(), RDSEngine.MYSQL.getEngineName(), engineVersion);
    verifySingleError(ENGINE_VERSION, INVALID_ENGINE_VERSION, engineVersion);
  }

  @Test
  public void testCheckInstanceClass() {
    checkInstanceClass("db.foo");
    verifyClean();
  }

  @Test
  public void testCheckInstanceClass_InvalidInstanceClass() {
    String instanceClass = "m3.large";
    checkInstanceClass(instanceClass);
    verifySingleError(INSTANCE_CLASS, INVALID_INSTANCE_CLASS, instanceClass);
  }

  @Test
  public void testCheckAllocatedStorage() {
    checkAllocatedStorage(TEST_ALLOCATED_STORAGE);
    verifyClean();
  }

  @Test
  public void testCheckAllocatedStorage_InvalidFormat() {
    String allocatedStorage = "x";
    checkAllocatedStorage(allocatedStorage);
    verifySingleError(ALLOCATED_STORAGE, INVALID_ALLOCATED_STORAGE_FORMAT_MSG, allocatedStorage);
  }

  @Test
  public void testCheckAllocatedStorage_AllocatedStorageTooSmall() {
    int allocatedStorage = MINIMUM_ALLOCATED_STORAGE - 1;
    checkAllocatedStorage(allocatedStorage);
    verifySingleError(ALLOCATED_STORAGE, ALLOCATED_STORAGE_TOO_SMALL, allocatedStorage);
  }

  @Test
  public void testCheckAllocatedStorage_AllocatedStorageTooLarge() {
    int allocatedStorage = MAXIMUM_ALLOCATED_STORAGE + 1;
    checkAllocatedStorage(allocatedStorage);
    verifySingleError(ALLOCATED_STORAGE, ALLOCATED_STORAGE_TOO_LARGE, allocatedStorage);
  }

  @Test
  public void testCheckDBSubnetGroupName() {
    mockDescribeDBSubnetGroups(TEST_DB_SUBNET_GROUP_NAME, true, true);
    checkDBSubnetGroupName(TEST_DB_SUBNET_GROUP_NAME);
    verifyClean();
  }

  @Test
  public void testCheckDBSubnetGroupName_MissingDBSubnetGroup() {
    mockDescribeDBSubnetGroups(TEST_DB_SUBNET_GROUP_NAME, false, true);
    checkDBSubnetGroupName(TEST_DB_SUBNET_GROUP_NAME);
    verifySingleError(DB_SUBNET_GROUP_NAME, DB_SUBNET_GROUP_NOT_FOUND, TEST_DB_SUBNET_GROUP_NAME);
  }

  @Test
  public void testCheckDBSubnetGroupName_InvalidDBSubnetGroup() {
    mockDescribeDBSubnetGroups(TEST_DB_SUBNET_GROUP_NAME, true, false);
    checkDBSubnetGroupName(TEST_DB_SUBNET_GROUP_NAME);
    verifySingleError(DB_SUBNET_GROUP_NAME, INVALID_DB_SUBNET_GROUP, TEST_DB_SUBNET_GROUP_NAME);
  }

  @Test
  public void testCheckMultiAZ() throws Exception {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(MULTI_AZ.unwrap().getConfigKey(), "true");
    configMap.put(AVAILABILITY_ZONE.unwrap().getConfigKey(), "us-east-1");
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkMultiAz(rdsClient, configuration, accumulator, localizationContext);

    verifySingleError(AVAILABILITY_ZONE, AVAILABILITY_ZONE_NOT_ALLOWED_FOR_MULTI_AZ, "us-east-1");
  }

  @Test
  public void testCheckStorageEncryption_Pass() {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(INSTANCE_CLASS.unwrap().getConfigKey(), "db.x1.large");
    configMap.put(STORAGE_ENCRYPTED.unwrap().getConfigKey(), "true");
    Configured configuration = new SimpleConfiguration(configMap);

    when(rdsEncryptionInstanceClasses.apply("db.x1.large")).thenReturn(Boolean.TRUE);

    validator.checkStorageEncryption(configuration, accumulator, localizationContext);
    verifyClean();
  }

  @Test
  public void testCheckStorageEncryption_Fail() {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(INSTANCE_CLASS.unwrap().getConfigKey(), "db.x1.large");
    configMap.put(STORAGE_ENCRYPTED.unwrap().getConfigKey(), "true");
    Configured configuration = new SimpleConfiguration(configMap);

    when(rdsEncryptionInstanceClasses.apply("db.x1.large")).thenReturn(Boolean.FALSE);

    validator.checkStorageEncryption(configuration, accumulator, localizationContext);
    verifySingleError(STORAGE_ENCRYPTED, ENCRYPTION_NOT_SUPPORTED, "db.x1.large");
  }

  /**
   * Invokes checkIdentifierFormat with the specified configuration.
   *
   * @param identifier    the database identifier
   * @param propertyToken the configuration property token, for error reporting
   */
  protected void checkIdentifierFormat(String identifier, ConfigurationPropertyToken propertyToken) {
    validator.checkIdentifierFormat(identifier, accumulator, propertyToken, localizationContext);
  }

  /**
   * Invokes checkIdentifierUniqueness with the specified identifier.
   *
   * @param identifier    the database identifier
   * @param propertyToken the configuration property token, for error reporting
   */
  protected void checkIdentifierUniqueness(String identifier,
      ConfigurationPropertyToken propertyToken) {
    validator.checkIdentifierUniqueness(rdsClient, identifier, propertyToken, accumulator,
        localizationContext);
  }

  /**
   * Invokes checkMasterUserPassword with the specified configuration.
   *
   * @param password the master user password
   */
  protected void checkMasterUserPassword(String password) {
    Map<String, String> configMap = Maps.newHashMap();
    if (password != null) {
      configMap.put(MASTER_USER_PASSWORD.unwrap().getConfigKey(), password);
    }
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkMasterUserPassword(configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkEngine with the specified configuration.
   *
   * @param databaseType  the database type
   * @param engineVersion the engine version
   */
  protected void checkEngine(DatabaseType databaseType, String engineVersion) {
    String databaseTypeName = databaseType.name();
    checkEngine(databaseTypeName, null, engineVersion);
    for (String engineName : RDSEngine.getSupportedEngineNames(databaseType)) {
      checkEngine(databaseTypeName, engineName, engineVersion);
    }
  }

  /**
   * Invokes checkEngine with the specified configuration.
   *
   * @param databaseType  the database type
   * @param engineName    the database engine name
   * @param engineVersion the engine version
   */
  protected void checkEngine(String databaseType, String engineName, String engineVersion) {
    Map<String, String> configMap = Maps.newHashMap();
    if (databaseType != null) {
      configMap.put(TYPE.unwrap().getConfigKey(), databaseType);
    }
    if (engineName != null) {
      configMap.put(ENGINE.unwrap().getConfigKey(), engineName);
    }
    configMap.put(ENGINE_VERSION.unwrap().getConfigKey(), engineVersion);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkEngine(rdsClient, configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkAllocatedStorage with the specified configuration.
   *
   * @param allocatedStorage the allocated storage
   */
  protected void checkAllocatedStorage(int allocatedStorage) {
    checkAllocatedStorage(String.valueOf(allocatedStorage));
  }

  /**
   * Invokes checkAllocatedStorage with the specified configuration.
   *
   * @param allocatedStorage the allocated storage
   */
  protected void checkAllocatedStorage(String allocatedStorage) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(ALLOCATED_STORAGE.unwrap().getConfigKey(), allocatedStorage);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkAllocatedStorage(configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkInstanceClass with the specified configuration.
   *
   * @param instanceClass the instance class
   */
  protected void checkInstanceClass(String instanceClass) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(INSTANCE_CLASS.unwrap().getConfigKey(), instanceClass);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkInstanceClass(configuration, accumulator, localizationContext);
  }

  /**
   * Invokes checkDBSubnetGroupName with the specified configuration.
   *
   * @param dbSubnetGroupName the DB subnet group name
   */
  protected void checkDBSubnetGroupName(String dbSubnetGroupName) {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put(DB_SUBNET_GROUP_NAME.unwrap().getConfigKey(), dbSubnetGroupName);
    Configured configuration = new SimpleConfiguration(configMap);
    validator.checkDBSubnetGroupName(rdsClient, configuration, accumulator, localizationContext);
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains no errors or
   * warnings.
   */
  private void verifyClean() {
    Map<String, Collection<PluginExceptionCondition>> conditionsByKey =
        accumulator.getConditionsByKey();
    assertThat(conditionsByKey).isEmpty();
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains exactly
   * one condition, which must be an error associated with the specified property.
   *
   * @param token the configuration property token for the property which should be in error
   */
  private void verifySingleError(ConfigurationPropertyToken token) {
    verifySingleError(token, Optional.<String>absent());
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains exactly
   * one condition, which must be an error with the specified message and associated with the
   * specified property.
   *
   * @param token    the configuration property token for the property which should be in error
   * @param errorMsg the expected error message
   * @param args     the error message arguments
   */
  private void verifySingleError(ConfigurationPropertyToken token,
      String errorMsg, Object... args) {
    verifySingleError(token, Optional.of(errorMsg), args);
  }

  /**
   * Verifies that the specified plugin exception condition accumulator contains exactly
   * one condition, which must be an error with the specified message and associated with the
   * specified property.
   *
   * @param token          the configuration property token for the property which should be in error
   * @param errorMsgFormat the expected error message
   * @param args           the error message arguments
   */
  private void verifySingleError(ConfigurationPropertyToken token,
      Optional<String> errorMsgFormat, Object... args) {
    Map<String, Collection<PluginExceptionCondition>> conditionsByKey =
        accumulator.getConditionsByKey();
    assertThat(conditionsByKey).hasSize(1);
    String configKey = token.unwrap().getConfigKey();
    assertThat(conditionsByKey.containsKey(configKey)).isTrue();
    Collection<PluginExceptionCondition> keyConditions = conditionsByKey.get(configKey);
    assertThat(keyConditions).hasSize(1);
    PluginExceptionCondition condition = keyConditions.iterator().next();
    verifySingleErrorCondition(condition, errorMsgFormat, args);
  }

  /**
   * Verifies that the specified plugin exception condition is an error with the specified message.
   *
   * @param condition      the plugin exception condition
   * @param errorMsgFormat the expected error message format
   * @param args           the error message arguments
   */
  private void verifySingleErrorCondition(PluginExceptionCondition condition,
      Optional<String> errorMsgFormat, Object... args) {
    assertThat(condition.isError()).isTrue();
    if (errorMsgFormat.isPresent()) {
      assertThat(condition.getMessage()).isEqualTo(String.format(errorMsgFormat.get(), args));
    }
  }
}
