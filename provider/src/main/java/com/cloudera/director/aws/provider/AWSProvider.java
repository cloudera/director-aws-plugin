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

package com.cloudera.director.aws.provider;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.cloudera.director.aws.AWSClientConfig;
import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.STSRoles;
import com.cloudera.director.aws.clientprovider.AWSKMSClientProvider;
import com.cloudera.director.aws.clientprovider.AmazonAutoScalingClientProvider;
import com.cloudera.director.aws.clientprovider.AmazonEC2ClientProvider;
import com.cloudera.director.aws.clientprovider.AmazonIdentityManagementClientProvider;
import com.cloudera.director.aws.clientprovider.AmazonRDSClientProvider;
import com.cloudera.director.aws.clientprovider.ClientProvider;
import com.cloudera.director.aws.ec2.EphemeralDeviceMappings;
import com.cloudera.director.aws.ec2.VirtualizationMappings;
import com.cloudera.director.aws.ec2.ebs.EBSDeviceMappings;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.ec2.provider.EC2Provider;
import com.cloudera.director.aws.ec2.provider.EC2ProviderConfigurationValidator;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.rds.RDSEncryptionInstanceClasses;
import com.cloudera.director.aws.rds.RDSEndpoints;
import com.cloudera.director.aws.rds.provider.RDSProvider;
import com.cloudera.director.aws.rds.provider.RDSProviderConfigurationValidator;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.CompositeConfigurationValidator;
import com.cloudera.director.spi.v2.provider.CloudProviderMetadata;
import com.cloudera.director.spi.v2.provider.ResourceProvider;
import com.cloudera.director.spi.v2.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v2.provider.util.AbstractCloudProvider;
import com.cloudera.director.spi.v2.provider.util.SimpleCloudProviderMetadataBuilder;

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
  public static final CloudProviderMetadata METADATA = new SimpleCloudProviderMetadataBuilder()
      .id(ID)
      .name("Amazon Web Services (AWS)")
      .description("Amazon Web Services (AWS) cloud provider implementation")
      .configurationProperties(Collections.emptyList())
      .credentialsProviderMetadata(AWSCredentialsProviderChainProvider.METADATA)
      .resourceProviderMetadata(RESOURCE_PROVIDER_METADATA)
      .build();

  /**
   * Returns the provider chain for the specified configuration.
   *
   * @param stsRoles                 the STS roles
   * @param configuration            the configuration
   * @param cloudLocalizationContext the parent cloud localization context
   * @return the provider chain for the specified configuration
   */
  protected static AWSCredentialsProvider getCredentialsProvider(
      STSRoles stsRoles,
      Configured configuration,
      LocalizationContext cloudLocalizationContext) {
    stsRoles = firstNonNull(stsRoles, STSRoles.DEFAULT);
    return new AWSCredentialsProviderChainProvider(stsRoles.getRoleConfigurations())
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
   * The EBS device mappings.
   */
  private final EBSDeviceMappings ebsDeviceMappings;

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
  private final ClientConfiguration clientConfiguration;

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
   * An Amazon EC2 client provider.
   */
  private final ClientProvider<AmazonEC2AsyncClient> amazonEC2ClientProvider;

  /**
   * An Amazon Auto Scaling client provider.
   */
  private final ClientProvider<AmazonAutoScalingAsyncClient> amazonAutoScalingClientProvider;

  /**
   * An Amazon identity management client provider.
   */
  private final ClientProvider<AmazonIdentityManagementClient> amazonIdentityManagementClientProvider;

  /**
   * An Amazon KMS client provider.
   */
  private final ClientProvider<AWSKMSClient> awskmsClientProvider;

  /**
   * An Amazon RDS client provider.
   */
  private final ClientProvider<AmazonRDSClient> amazonRDSClientProvider;

  /**
   * Whether to use tag on create.
   */
  private final boolean useTagOnCreate;

  /**
   * Creates an AWS provider with the specified parameters.
   *
   * @param configuration                the configuration
   * @param ephemeralDeviceMappings      the ephemeral device mappings
   * @param ebsDeviceMappings            the ebs device mappings
   * @param ebsMetadata                  the EBS metadata
   * @param virtualizationMappings       the virtualization mappings
   * @param rdsEndpoints                 the RDS endpoints
   * @param rdsEncryptionInstanceClasses the RDS encryption instance classes
   * @param awsClientConfig              the AWS client configuration
   * @param awsFilters                   the AWS filters
   * @param awsTimeouts                  the AWS timeouts
   * @param customTagMappings            the custom tag mappings
   * @param networkRules                 the network rules
   * @param stsRoles                     the STS roles
   * @param useTagOnCreate               whether to use tag on create
   * @param rootLocalizationContext      the root localization context
   */
  public AWSProvider(Configured configuration,
      EphemeralDeviceMappings ephemeralDeviceMappings,
      EBSDeviceMappings ebsDeviceMappings,
      EBSMetadata ebsMetadata,
      VirtualizationMappings virtualizationMappings,
      RDSEndpoints rdsEndpoints,
      RDSEncryptionInstanceClasses rdsEncryptionInstanceClasses,
      AWSClientConfig awsClientConfig, AWSFilters awsFilters,
      AWSTimeouts awsTimeouts,
      CustomTagMappings customTagMappings,
      NetworkRules networkRules,
      STSRoles stsRoles,
      boolean useTagOnCreate,
      LocalizationContext rootLocalizationContext) {
    this(configuration, ephemeralDeviceMappings, ebsDeviceMappings, ebsMetadata,
        virtualizationMappings, rdsEndpoints, rdsEncryptionInstanceClasses,
        awsClientConfig,
        awsFilters, awsTimeouts, customTagMappings, networkRules,
        getCredentialsProvider(stsRoles, configuration, METADATA.getLocalizationContext(rootLocalizationContext)),
        useTagOnCreate, rootLocalizationContext);
  }

  /**
   * Creates an AWS provider with the specified parameters.
   *
   * @param configuration                the configuration
   * @param ephemeralDeviceMappings      the ephemeral device mappings
   * @param ebsDeviceMappings            the ebs device mappings
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
   * @param useTagOnCreate               whether to use tag on create
   * @param rootLocalizationContext      the root localization context
   */
  @SuppressWarnings({"PMD.UnusedFormalParameter", "UnusedParameters"})
  public AWSProvider(
      Configured configuration,
      EphemeralDeviceMappings ephemeralDeviceMappings,
      EBSDeviceMappings ebsDeviceMappings,
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
      boolean useTagOnCreate,
      LocalizationContext rootLocalizationContext) {
    super(METADATA, rootLocalizationContext);
    this.credentialsProvider =
        checkNotNull(credentialsProvider, "credentialsProvider is null");
    this.ephemeralDeviceMappings = ephemeralDeviceMappings;
    this.ebsDeviceMappings = ebsDeviceMappings;
    this.ebsMetadata = ebsMetadata;
    this.virtualizationMappings = virtualizationMappings;
    this.rdsEndpoints = rdsEndpoints;
    this.rdsEncryptionInstanceClasses = rdsEncryptionInstanceClasses;
    this.clientConfiguration = getClientConfiguration(awsClientConfig);
    this.awsFilters = checkNotNull(awsFilters, "awsFilters is null");
    this.awsTimeouts = checkNotNull(awsTimeouts, "awsTimeouts is null");
    this.customTagMappings = checkNotNull(customTagMappings, "customTagMappings is null");
    this.networkRules = checkNotNull(networkRules, "networkRules is null");
    this.useTagOnCreate = useTagOnCreate;

    this.amazonEC2ClientProvider = new AmazonEC2ClientProvider(
        this.credentialsProvider, this.clientConfiguration);
    this.amazonAutoScalingClientProvider = new AmazonAutoScalingClientProvider(
        this.credentialsProvider, this.clientConfiguration);
    this.amazonIdentityManagementClientProvider = new AmazonIdentityManagementClientProvider(
        this.credentialsProvider, this.clientConfiguration);
    this.awskmsClientProvider = new AWSKMSClientProvider(
        this.credentialsProvider, this.clientConfiguration);
    this.amazonRDSClientProvider = new AmazonRDSClientProvider(
        this.credentialsProvider, this.clientConfiguration, this.rdsEndpoints);
  }

  @Override
  protected ConfigurationValidator getResourceProviderConfigurationValidator(
      ResourceProviderMetadata resourceProviderMetadata) {
    ConfigurationValidator providerSpecificValidator;

    if (resourceProviderMetadata.getId().equals(EC2Provider.METADATA.getId())) {
      providerSpecificValidator = new EC2ProviderConfigurationValidator(
          amazonEC2ClientProvider,
          amazonIdentityManagementClientProvider,
          awskmsClientProvider);

    } else if (resourceProviderMetadata.getId().equals(RDSProvider.METADATA.getId())) {
      providerSpecificValidator = new RDSProviderConfigurationValidator(amazonRDSClientProvider);

    } else {
      throw new IllegalArgumentException("No such provider: " + resourceProviderMetadata.getId());
    }

    return new CompositeConfigurationValidator(
        METADATA.getProviderConfigurationValidator(),
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
    LocalizationContext localizationContext = getLocalizationContext();
    return new EC2Provider(target, ephemeralDeviceMappings, ebsDeviceMappings, ebsMetadata,
        virtualizationMappings, awsFilters, awsTimeouts, customTagMappings, networkRules,
        amazonEC2ClientProvider, amazonAutoScalingClientProvider,
        amazonIdentityManagementClientProvider, awskmsClientProvider,
        useTagOnCreate, localizationContext);
  }

  /**
   * Creates an RDS provider with the specified configuration.
   *
   * @param target the configuration
   * @return the RDS provider
   */
  protected RDSProvider createRDSProvider(Configured target) {
    LocalizationContext localizationContext = getLocalizationContext();
    return new RDSProvider(target, rdsEncryptionInstanceClasses,
        amazonRDSClientProvider, amazonIdentityManagementClientProvider,
        customTagMappings, localizationContext);
  }

  /**
   * Returns the AWS client configuration.
   *
   * @return the AWS client configuration
   */
  private static ClientConfiguration getClientConfiguration(AWSClientConfig awsClientConfig) {
    return (awsClientConfig == null)
        ? AWSClientConfig.DEFAULT_CLIENT_CONFIG
        : awsClientConfig.getClientConfiguration();
  }
}
