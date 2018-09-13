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

package com.cloudera.director.aws.rds.provider;

import static com.cloudera.director.aws.rds.RDSEngine.getSupportedEngineNamesByDatabaseType;
import static java.util.Objects.requireNonNull;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.CreateDBInstanceRequest;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DBInstanceNotFoundException;
import com.amazonaws.services.rds.model.DeleteDBInstanceRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.amazonaws.services.rds.model.Tag;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.clientprovider.ClientProvider;
import com.cloudera.director.aws.rds.RDSEncryptionInstanceClasses;
import com.cloudera.director.aws.rds.RDSInstance;
import com.cloudera.director.aws.rds.RDSInstanceState;
import com.cloudera.director.aws.rds.RDSInstanceTemplate;
import com.cloudera.director.aws.rds.RDSInstanceTemplateConfigurationValidator;
import com.cloudera.director.aws.rds.RDSProviderConfigurationPropertyToken;
import com.cloudera.director.aws.rds.RDSStatus;
import com.cloudera.director.aws.rds.RDSTagHelper;
import com.cloudera.director.spi.v2.database.DatabaseServerProviderMetadata;
import com.cloudera.director.spi.v2.database.util.AbstractDatabaseServerProvider;
import com.cloudera.director.spi.v2.database.util.SimpleDatabaseServerProviderMetadata;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.Resource;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.CompositeConfigurationValidator;
import com.cloudera.director.spi.v2.util.ConfigurationPropertiesUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database server provider of Amazon RDS instances.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class RDSProvider extends AbstractDatabaseServerProvider<RDSInstance, RDSInstanceTemplate> {

  private static final Logger LOG = LoggerFactory.getLogger(RDSProvider.class);

  /**
   * The provider configuration properties.
   */
  protected static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
      ConfigurationPropertiesUtil.asConfigurationPropertyList(
          RDSProviderConfigurationPropertyToken.values());

  /**
   * The resource provider ID.
   */
  public static final String ID = RDSProvider.class.getCanonicalName();

  /**
   * The resource provider metadata.
   */
  public static final DatabaseServerProviderMetadata METADATA =
      SimpleDatabaseServerProviderMetadata.databaseServerProviderMetadataBuilder()
          .id(ID)
          .name("RDS (Relational Database Service)")
          .description("AWS RDS database server provider")
          .providerClass(RDSProvider.class)
          .providerConfigurationProperties(CONFIGURATION_PROPERTIES)
          .resourceTemplateConfigurationProperties(RDSInstanceTemplate.getConfigurationProperties())
          .resourceDisplayProperties(RDSInstance.getDisplayProperties())
          .supportedDatabaseTypes(getSupportedEngineNamesByDatabaseType().keySet())
          .build();

  private final AmazonRDSClient client;

  @SuppressWarnings("PMD.UnusedPrivateField")
  private final AmazonIdentityManagementClient identityManagementClient;

  private final boolean associatePublicIpAddresses;

  private final ConfigurationValidator resourceTemplateConfigurationValidator;

  private final RDSTagHelper rdsTagHelper;

  /**
   * Construct a new provider instance and validate all configurations.
   *
   * @param configuration                    the configuration
   * @param encryptionInstanceClasses        the RDS encryption instance classes
   * @param clientProvider                   the RDS client provider
   * @param identityManagementClientProvider the AIM client provider
   * @param customTagMappings                the custom tag mappings
   * @param cloudLocalizationContext         the parent cloud localization context
   */
  public RDSProvider(
      Configured configuration,
      RDSEncryptionInstanceClasses encryptionInstanceClasses,
      ClientProvider<AmazonRDSClient> clientProvider,
      ClientProvider<AmazonIdentityManagementClient> identityManagementClientProvider,
      CustomTagMappings customTagMappings,
      LocalizationContext cloudLocalizationContext) {
    super(configuration, METADATA, cloudLocalizationContext);
    LocalizationContext localizationContext = getLocalizationContext();
    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();

    this.client = requireNonNull(clientProvider, "clientProvider is null")
        .getClient(configuration, accumulator, localizationContext, false);
    this.identityManagementClient = requireNonNull(
        identityManagementClientProvider, "identityManagementClientProvider is null")
        .getClient(configuration, accumulator, localizationContext, false);

    if (accumulator.hasError()) {
      PluginExceptionDetails pluginExceptionDetails =
          new PluginExceptionDetails(accumulator.getConditionsByKey());
      throw new UnrecoverableProviderException("Provider initialization failed",
          pluginExceptionDetails);
    }

    this.associatePublicIpAddresses = Boolean.parseBoolean(
        getConfigurationValue(RDSProviderConfigurationPropertyToken.ASSOCIATE_PUBLIC_IP_ADDRESSES,
            localizationContext));

    this.resourceTemplateConfigurationValidator =
        new CompositeConfigurationValidator(METADATA.getResourceTemplateConfigurationValidator(),
            new RDSInstanceTemplateConfigurationValidator(this, encryptionInstanceClasses));

    this.rdsTagHelper = new RDSTagHelper(customTagMappings);
  }

  /**
   * Returns the RDS client.
   *
   * @return the RDS client
   */
  public AmazonRDSClient getClient() {
    return client;
  }

  @Override
  public ConfigurationValidator getResourceTemplateConfigurationValidator() {
    return resourceTemplateConfigurationValidator;
  }

  @Override
  public Resource.Type getResourceType() {
    return RDSInstance.TYPE;
  }

  @Override
  public RDSInstanceTemplate createResourceTemplate(
      String name, Configured configuration, Map<String, String> tags) {
    return new RDSInstanceTemplate(name, configuration, tags, getLocalizationContext());
  }

  @Override
  public Collection<RDSInstance> allocate(RDSInstanceTemplate template, Collection<String> virtualInstanceIds,
      int minCount) throws InterruptedException {
    int instanceCount = virtualInstanceIds.size();

    LOG.info(">> Requesting {} instances for {}", instanceCount, template);
    List<RDSInstance> rdsInstances = Lists.newArrayListWithCapacity(virtualInstanceIds.size());
    for (String virtualInstanceId : virtualInstanceIds) {
      CreateDBInstanceRequest request = buildCreateRequest(template, virtualInstanceId);
      rdsInstances.add(new RDSInstance(template, virtualInstanceId, client.createDBInstance(request)));
    }
    return rdsInstances;
  }

  @Override
  public Collection<RDSInstance> find(final RDSInstanceTemplate template,
      Collection<String> virtualInstanceIds) throws InterruptedException {

    final Collection<RDSInstance> rdsInstances =
        Lists.newArrayListWithExpectedSize(virtualInstanceIds.size());

    forEachInstance(virtualInstanceIds, dbInstance -> {
      String virtualInstanceId = checkInstanceIsManagedByDirector(dbInstance, template);
      rdsInstances.add(new RDSInstance(template, virtualInstanceId, dbInstance));
    });

    return rdsInstances;
  }

  @Override
  public void delete(RDSInstanceTemplate template, Collection<String> virtualInstanceIds)
      throws InterruptedException {

    if (virtualInstanceIds.isEmpty()) {
      return;
    }

    for (String virtualInstanceId : virtualInstanceIds) {
      LOG.info(">> Terminating {}", virtualInstanceId);

      DeleteDBInstanceRequest request = new DeleteDBInstanceRequest()
          .withDBInstanceIdentifier(virtualInstanceId);
      if (template.isSkipFinalSnapshot().or(false)) {
        request.setSkipFinalSnapshot(true);
      } else {
        String snapshotIdentifier = String.format("%s-director-final-snapshot-%d",
            virtualInstanceId, System.currentTimeMillis());
        request.setFinalDBSnapshotIdentifier(snapshotIdentifier);
      }

      try {
        DBInstance deletedDbInstance = client.deleteDBInstance(request);
        LOG.info("<< Result {}", deletedDbInstance);
      } catch (DBInstanceNotFoundException e) {
        LOG.warn("<< Instance {} was not found, assuming already deleted", virtualInstanceId);
      }
    }
  }

  @Override
  public Map<String, InstanceState> getInstanceState(
      RDSInstanceTemplate template, Collection<String> virtualInstanceIds) {
    Map<String, InstanceState> instanceStateByVirtualInstanceId =
        Maps.newHashMapWithExpectedSize(virtualInstanceIds.size());

    // RDS does not allow batching of DB instance status requests.
    for (String virtualInstanceId : virtualInstanceIds) {
      InstanceState instanceState;
      try {
        DescribeDBInstancesResult result = client.describeDBInstances(
            new DescribeDBInstancesRequest()
                .withDBInstanceIdentifier(virtualInstanceId));
        LOG.info("<< Result: {}", result);

        // Paging not required, should only ever be one instance returned
        if (result.getDBInstances().size() > 0) {
          DBInstance dbInstance = result.getDBInstances().get(0);
          RDSStatus status = RDSStatus.valueOfRDSString(dbInstance.getDBInstanceStatus());
          instanceState = RDSInstanceState.fromRdsStatus(status);
        } else {
          instanceState = RDSInstanceState.fromRdsStatus(null);
        }
      } catch (DBInstanceNotFoundException e) {
        instanceState = RDSInstanceState.fromRdsStatus(null);
      }
      instanceStateByVirtualInstanceId.put(virtualInstanceId, instanceState);
    }

    return instanceStateByVirtualInstanceId;
  }

  /**
   * Returns a DB instance request based on the specified template.
   *
   * @param template          the template
   * @param virtualInstanceId the virtual instance ID
   * @return a DB instance request based on the specified template
   */
  private CreateDBInstanceRequest buildCreateRequest(RDSInstanceTemplate template,
      String virtualInstanceId) {
    CreateDBInstanceRequest request = new CreateDBInstanceRequest()
        .withDBInstanceIdentifier(virtualInstanceId)
        .withDBInstanceClass(template.getInstanceClass())
        .withDBSubnetGroupName(template.getDbSubnetGroupName())
        .withVpcSecurityGroupIds(template.getVpcSecurityGroupIds())
        .withPubliclyAccessible(associatePublicIpAddresses)
        .withAllocatedStorage(template.getAllocatedStorage())
        .withEngine(template.getEngine())
        .withMasterUsername(template.getAdminUser())
        .withMasterUserPassword(template.getAdminPassword())  // masterPassword in AWS SDK 1.9+
        .withTags(convertToTags(template, virtualInstanceId));

    if (template.getEngineVersion().isPresent()) {
      request = request.withEngineVersion(template.getEngineVersion().get());
    }
    if (template.getAvailabilityZone().isPresent()) {
      request = request.withAvailabilityZone(template.getAvailabilityZone().get());
    }
    if (template.getAutoMinorVersionUpgrade().isPresent()) {
      request = request.withAutoMinorVersionUpgrade(template.getAutoMinorVersionUpgrade().get());
    }
    if (template.getBackupRetentionPeriod().isPresent()) {
      request = request.withBackupRetentionPeriod(template.getBackupRetentionPeriod().get());
    }
    if (template.getDbName().isPresent()) {
      request = request.withDBName(template.getDbName().get());
    }
    if (template.getDbParameterGroupName().isPresent()) {
      request = request.withDBParameterGroupName(template.getDbParameterGroupName().get());
    }
    if (template.getEngineVersion().isPresent()) {
      request = request.withEngineVersion(template.getEngineVersion().get());
    }
    if (template.getLicenseModel().isPresent()) {
      request = request.withLicenseModel(template.getLicenseModel().get());
    }
    if (template.isMultiAZ().isPresent()) {
      request = request.withMultiAZ(template.isMultiAZ().get());
    }
    if (template.getOptionGroupName().isPresent()) {
      request = request.withOptionGroupName(template.getOptionGroupName().get());
    }
    if (template.getPort().isPresent()) {
      request = request.withPort(template.getPort().get());
    }
    if (template.getPreferredBackupWindow().isPresent()) {
      request = request.withPreferredBackupWindow(template.getPreferredBackupWindow().get());
    }
    if (template.getPreferredMaintenanceWindow().isPresent()) {
      request =
          request.withPreferredMaintenanceWindow(template.getPreferredMaintenanceWindow().get());
    }
    if (template.isPubliclyAccessible().isPresent()) {
      request = request.withPubliclyAccessible(template.isPubliclyAccessible().get());
    }
    if (template.isStorageEncrypted().isPresent()) {
      request = request.withStorageEncrypted(template.isStorageEncrypted().get());
    }

    return request;
  }

  private Collection<Tag> convertToTags(RDSInstanceTemplate template, String instanceId) {
    List<Tag> userDefinedTags = rdsTagHelper.getUserDefinedTags(template);
    return rdsTagHelper.getInstanceTags(template, instanceId, userDefinedTags);
  }

  /**
   * Performs a sequence of strict instance ownership checks to avoid any potential harmful
   * accidents.
   *
   * @param instance the instance
   * @param template the template from which the instance was created, or <code>null</code>
   *                 if it is unknown (such as during a delete call)
   * @return the virtual instance ID
   */
  @SuppressWarnings("PMD.UnusedFormalParameter")
  private String checkInstanceIsManagedByDirector(DBInstance instance,
      RDSInstanceTemplate template) {
    // TODO perform any desired tag validation
    return instance.getDBInstanceIdentifier();
  }

  /**
   * Represents a callback that can be applied to each instance of
   * a {@code DescribeDBInstancesResult}.
   */
  private interface InstanceHandler {

    /**
     * Handles the specified instance.
     *
     * @param dbInstance the instance
     */
    void handle(DBInstance dbInstance);
  }

  /**
   * Iterates through the instances identified by the specified virtual instance IDs
   * and calls the specified handler on each instance.
   *
   * @param virtualInstanceIds the virtual instance IDs
   * @param instanceHandler    the instance handler
   */
  private void forEachInstance(Collection<String> virtualInstanceIds,
      RDSProvider.InstanceHandler instanceHandler) {
    // TODO RDS does not currently support the withFilters parameter, so we have to load one at a
    // time by id
    //DescribeDBInstancesResult result = client.describeDBInstances(new DescribeDBInstancesRequest()
    //    .withFilters(new Filter().withFilterName("tag:" + Tags.CLOUDERA_DIRECTOR_ID)
    //        .withFilterValue(virtualInstanceIds)));
    for (String virtualInstanceId : virtualInstanceIds) {
      DescribeDBInstancesResult result = client.describeDBInstances(new DescribeDBInstancesRequest()
          .withDBInstanceIdentifier(virtualInstanceId));
      forEachInstance(result, instanceHandler);
    }
  }

  /**
   * Iterates through the instances in the specified {@code DescribeInstancesResult}
   * and calls the specified handler on each instance.
   *
   * @param result          the {@code DescribeInstancesResult}
   * @param instanceHandler the instance handler
   */
  private void forEachInstance(DescribeDBInstancesResult result, InstanceHandler instanceHandler) {
    for (DBInstance dbInstance : result.getDBInstances()) {
      instanceHandler.handle(dbInstance);
    }
  }
}
