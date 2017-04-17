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

package com.cloudera.director.aws;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.cloudera.director.aws.ec2.EC2Provider;
import com.cloudera.director.aws.ec2.EC2ProviderConfigurationValidator;
import com.cloudera.director.aws.ec2.EphemeralDeviceMappings;
import com.cloudera.director.aws.ec2.VirtualizationMappings;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.rds.RDSEncryptionInstanceClasses;
import com.cloudera.director.aws.rds.RDSEndpoints;
import com.cloudera.director.aws.rds.RDSProvider;
import com.cloudera.director.aws.rds.RDSProviderConfigurationValidator;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.CompositeConfigurationValidator;
import com.cloudera.director.spi.v1.provider.CloudProviderMetadata;
import com.cloudera.director.spi.v1.provider.ResourceProvider;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.AbstractCloudProvider;
import com.cloudera.director.spi.v1.provider.util.SimpleCloudProviderMetadataBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * AWS cloud provider plugin.
 */
public class AWSProvider extends AbstractCloudProvider {

  /**
   * The cloud provider ID.
   */
  public static final String ID = "aws";

  /**
   * The resource provider metadata.
   */
  private static final List<ResourceProviderMetadata> RESOURCE_PROVIDER_METADATA =
      Collections.unmodifiableList(Arrays.asList(EC2Provider.METADATA, RDSProvider.METADATA));

  /**
   * The cloud provider metadata.
   */
  protected static final CloudProviderMetadata METADATA = new SimpleCloudProviderMetadataBuilder()
      .id(ID)
      .name("Amazon Web Services (AWS)")
      .description("Amazon Web Services (AWS) cloud provider implementation")
      .configurationProperties(Collections.<ConfigurationProperty>emptyList())
      .credentialsProviderMetadata(AWSCredentialsProviderChainProvider.METADATA)
      .resourceProviderMetadata(RESOURCE_PROVIDER_METADATA)
      .build();

  /**
   * Returns the provider chain for the specified configuration.
   *
   * @param configuration            the configuration
   * @param cloudLocalizationContext the parent cloud localization context
   * @return the provider chain for the specified configuration
   */
  protected static AWSCredentialsProvider getCredentialsProvider(Configured configuration,
      LocalizationContext cloudLocalizationContext) {
    return new AWSCredentialsProviderChainProvider()
        .createCredentials(configuration, cloudLocalizationContext);
  }

  /**
   * The AWS credentials provider.
   */
  private final AWSCredentialsProvider credentialsProvider;

  /**
   * The ephemeral device mappings.
   */
  private final EphemeralDeviceMappings ephemeralDeviceMappings;

  /**
   * The EBS metadata.
   */
  private final EBSMetadata ebsMetadata;

  /**
   * The virtualization mappings.
   */
  private final VirtualizationMappings virtualizationMappings;

  /**
   * The RDS endpoints.
   */
  private final RDSEndpoints rdsEndpoints;

  /**
   * The RDS encryption instance classes.
   */
  private final RDSEncryptionInstanceClasses rdsEncryptionInstanceClasses;

  /**
   * The AWS config.
   */
  private final AWSClientConfig awsClientConfig;

  /**
   * The AWS filters.
   */
  private final AWSFilters awsFilters;

  /**
   * The AWS timeouts.
   */
  private final AWSTimeouts awsTimeouts;

  /**
   * The custom tag mappings.
   */
  private final CustomTagMappings customTagMappings;

  /**
   * The network rules.
   */
  private final NetworkRules networkRules;

  /**
   * Creates an AWS provider with the specified parameters.
   *
   * @param configuration                the configuration
   * @param ephemeralDeviceMappings      the ephemeral device mappings
   * @param ebsMetadata                  the EBS metadata
   * @param virtualizationMappings       the virtualization mappings
   * @param rdsEndpoints                 the RDS endpoints
   * @param rdsEncryptionInstanceClasses the RDS encryption instance classes
   * @param awsClientConfig              the AWS client configuration
   * @param awsFilters                   the AWS filters
   * @param awsTimeouts                  the AWS timeouts
   * @param customTagMappings            the custom tag mappings
   * @param networkRules                 the network rules
   * @param rootLocalizationContext      the root localization context
   */
  public AWSProvider(Configured configuration,
      EphemeralDeviceMappings ephemeralDeviceMappings,
      EBSMetadata ebsMetadata,
      VirtualizationMappings virtualizationMappings,
      RDSEndpoints rdsEndpoints,
      RDSEncryptionInstanceClasses rdsEncryptionInstanceClasses,
      AWSClientConfig awsClientConfig, AWSFilters awsFilters,
      AWSTimeouts awsTimeouts,
      CustomTagMappings customTagMappings,
      NetworkRules networkRules,
      LocalizationContext rootLocalizationContext) {
    this(configuration, ephemeralDeviceMappings, ebsMetadata,
        virtualizationMappings, rdsEndpoints, rdsEncryptionInstanceClasses,
        awsClientConfig,
        awsFilters, awsTimeouts, customTagMappings, networkRules,
        getCredentialsProvider(configuration, METADATA.getLocalizationContext(rootLocalizationContext)),
        rootLocalizationContext);
  }

  /**
   * Creates an AWS provider with the specified parameters.
   *
   * @param configuration                the configuration
   * @param ephemeralDeviceMappings      the ephemeral device mappings
   * @param ebsMetadata                  the ebs metadata
   * @param virtualizationMappings       the virtualization mappings
   * @param rdsEndpoints                 the RDS endpoints
   * @param rdsEncryptionInstanceClasses the RDS encryption instance classes
   * @param awsClientConfig              the AWS client configuration
   * @param awsFilters                   the AWS filters
   * @param awsTimeouts                  the AWS timeouts
   * @param customTagMappings            the custom tag mappings
   * @param networkRules                 the network rules
   * @param credentialsProvider          the AWS credentials provider
   * @param rootLocalizationContext      the root localization context
   */
  @SuppressWarnings({"PMD.UnusedFormalParameter", "UnusedParameters"})
  public AWSProvider(Configured configuration,
      EphemeralDeviceMappings ephemeralDeviceMappings,
      EBSMetadata ebsMetadata,
      VirtualizationMappings virtualizationMappings,
      RDSEndpoints rdsEndpoints,
      RDSEncryptionInstanceClasses rdsEncryptionInstanceClasses,
      AWSClientConfig awsClientConfig,
      AWSFilters awsFilters,
      AWSTimeouts awsTimeouts,
      CustomTagMappings customTagMappings,
      NetworkRules networkRules,
      AWSCredentialsProvider credentialsProvider,
      LocalizationContext rootLocalizationContext) {
    super(METADATA, rootLocalizationContext);
    this.credentialsProvider =
        checkNotNull(credentialsProvider, "credentialsProvider is null");
    this.ephemeralDeviceMappings = ephemeralDeviceMappings;
    this.ebsMetadata = ebsMetadata;
    this.virtualizationMappings = virtualizationMappings;
    this.rdsEndpoints = rdsEndpoints;
    this.rdsEncryptionInstanceClasses = rdsEncryptionInstanceClasses;
    this.awsClientConfig = awsClientConfig;
    this.awsFilters = checkNotNull(awsFilters, "awsFilters is null");
    this.awsTimeouts = checkNotNull(awsTimeouts, "awsTimeouts is null");
    this.customTagMappings = checkNotNull(customTagMappings, "customTagMappings is null");
    this.networkRules = checkNotNull(networkRules, "networkRules is null");
  }

  @Override
  protected ConfigurationValidator getResourceProviderConfigurationValidator(
      ResourceProviderMetadata resourceProviderMetadata) {
    ClientConfiguration clientConfiguration = getClientConfiguration();
    ConfigurationValidator providerSpecificValidator;
    if (resourceProviderMetadata.getId().equals(EC2Provider.METADATA.getId())) {
      AmazonEC2Client client = new AmazonEC2Client(credentialsProvider, clientConfiguration);
      AmazonIdentityManagementClient identityManagementClient =
          new AmazonIdentityManagementClient(credentialsProvider, clientConfiguration);
      AWSKMSClient kmsClient =
          new AWSKMSClient(credentialsProvider, clientConfiguration);
      providerSpecificValidator =
          new EC2ProviderConfigurationValidator(client, identityManagementClient, kmsClient);
    } else if (resourceProviderMetadata.getId().equals(RDSProvider.METADATA.getId())) {
      AmazonRDSClient client = new AmazonRDSClient(credentialsProvider, clientConfiguration);
      providerSpecificValidator = new RDSProviderConfigurationValidator(client, rdsEndpoints);
    } else {
      throw new IllegalArgumentException("No such provider: " + resourceProviderMetadata.getId());
    }
    return new CompositeConfigurationValidator(METADATA.getProviderConfigurationValidator(),
        providerSpecificValidator);
  }

  @Override
  public ResourceProvider createResourceProvider(String resourceProviderId,
      Configured configuration) {
    ResourceProviderMetadata resourceProviderMetadata =
        getProviderMetadata().getResourceProviderMetadata(resourceProviderId);
    if (resourceProviderMetadata.getId().equals(EC2Provider.METADATA.getId())) {
      return createEC2Provider(configuration);
    } else if (resourceProviderMetadata.getId().equals(RDSProvider.METADATA.getId())) {
      return createRDSProvider(configuration);
    }
    throw new IllegalArgumentException("No such provider: " + resourceProviderMetadata.getId());
  }

  /**
   * Creates an EC2 provider with the specified configuration.
   *
   * @param target the configuration
   * @return the EC2 provider
   */
  protected EC2Provider createEC2Provider(Configured target) {
    ClientConfiguration clientConfiguration = getClientConfiguration();
    return new EC2Provider(target, ephemeralDeviceMappings, ebsMetadata,
        virtualizationMappings, awsFilters, awsTimeouts, customTagMappings, networkRules,
        new AmazonEC2Client(credentialsProvider, clientConfiguration),
        new AmazonIdentityManagementClient(credentialsProvider, clientConfiguration),
        new AWSKMSClient(credentialsProvider, clientConfiguration),
        getLocalizationContext());
  }

  /**
   * Creates an RDS provider with the specified configuration.
   *
   * @param target the configuration
   * @return the RDS provider
   */
  protected RDSProvider createRDSProvider(Configured target) {
    ClientConfiguration clientConfiguration = getClientConfiguration();
    return new RDSProvider(target, rdsEndpoints, rdsEncryptionInstanceClasses,
        new AmazonRDSClient(credentialsProvider, clientConfiguration),
        new AmazonIdentityManagementClient(credentialsProvider, clientConfiguration),
        customTagMappings, getLocalizationContext());
  }

  /**
   * Returns the AWS client configuration.
   *
   * @return the AWS client configuration
   */
  protected ClientConfiguration getClientConfiguration() {
    return (awsClientConfig == null)
        ? AWSClientConfig.DEFAULT_CLIENT_CONFIG
        : awsClientConfig.getClientConfiguration();
  }
}
