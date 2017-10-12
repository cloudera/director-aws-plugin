// (c) Copyright 2017 Cloudera, Inc.

package com.cloudera.director.aws.ec2;

import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.IMPORT_KEY_PAIR_IF_MISSING;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.KEY_NAME_PREFIX;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.ec2.ebs.EBSDeviceMappings;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.cloudera.director.aws.shaded.com.google.common.base.Optional;
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.common.AWSKMSClientProvider;
import com.cloudera.director.aws.common.AmazonEC2ClientProvider;
import com.cloudera.director.aws.common.AmazonIdentityManagementClientProvider;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;

import java.util.Locale;

public class EC2ProviderFixture {

  private Configured configured;
  private EphemeralDeviceMappings edMappings;
  private EBSDeviceMappings ebsMappings;
  private EBSMetadata ebsMetadata;
  private VirtualizationMappings vMappings;
  private AWSFilters filters;
  private AWSTimeouts timeouts;
  private CustomTagMappings tagMappings;
  private NetworkRules networkRules;
  private AmazonEC2ClientProvider ec2ClientProvider;
  private AmazonIdentityManagementClientProvider iamClientProvider;
  private AWSKMSClientProvider kmsClientProvider;
  private LocalizationContext localizationContext;

  private AWSFilters ec2Filters;
  private AWSFilters templateFilters;

  private AmazonEC2AsyncClient ec2Client;

  public EC2ProviderFixture() {
    configured = mock(Configured.class);
    edMappings = mock(EphemeralDeviceMappings.class);
    ebsMappings = mock(EBSDeviceMappings.class);
    ebsMetadata = mock(EBSMetadata.class);
    vMappings = mock(VirtualizationMappings.class);
    filters = mock(AWSFilters.class);
    timeouts = mock(AWSTimeouts.class);
    tagMappings = mock(CustomTagMappings.class);
    networkRules = NetworkRules.EMPTY_RULES;
    ec2ClientProvider = mock(AmazonEC2ClientProvider.class);
    iamClientProvider = mock(AmazonIdentityManagementClientProvider.class);
    kmsClientProvider = mock(AWSKMSClientProvider.class);
    localizationContext = new DefaultLocalizationContext(Locale.US, "test");

    // for EC2Provider itself
    ec2Filters = mock(AWSFilters.class);
    when(filters.getSubfilters(EC2Provider.ID)).thenReturn(ec2Filters);

    // for EC2InstanceTemplateConfigurationValidator
    templateFilters = mock(AWSFilters.class);
    when(ec2Filters.getSubfilters("template")).thenReturn(templateFilters);

    // for most things
    ec2Client = mock(AmazonEC2AsyncClient.class);
    when(ec2ClientProvider.getClient(eq(configured), any(PluginExceptionConditionAccumulator.class),
                                     any(LocalizationContext.class), eq(false)))
        .thenReturn(ec2Client);

    // for EbsAllocator
    when(timeouts.getTimeout(any(String.class))).thenReturn(Optional.<Long>absent());
  }

  public EC2Provider createEc2Provider() {
    return createEc2Provider(false, null);
  }

  public EC2Provider createEc2Provider(boolean importKeyPairIfMissing, String keyNamePrefix) {
    if (importKeyPairIfMissing) {
      when(configured.getConfigurationValue(eq(IMPORT_KEY_PAIR_IF_MISSING), any(LocalizationContext.class)))
          .thenReturn("true");
    }
    if (keyNamePrefix != null) {
      when(configured.getConfigurationValue(eq(KEY_NAME_PREFIX), any(LocalizationContext.class)))
          .thenReturn(keyNamePrefix);
    }
    return new EC2Provider(configured, edMappings, ebsMappings, ebsMetadata, vMappings,
                           filters, timeouts, tagMappings, networkRules,
                           ec2ClientProvider, iamClientProvider, kmsClientProvider,
                           localizationContext);
  }

  public Configured getConfigured() {
    return configured;
  }

  public EphemeralDeviceMappings getEphemeralDeviceMappings() {
    return edMappings;
  }

  public EBSMetadata getEbsMetadata() {
    return ebsMetadata;
  }

  public VirtualizationMappings getVirtualizationMappings() {
    return vMappings;
  }

  public AWSFilters getAwsFilters() {
    return filters;
  }

  public AWSTimeouts getAwsTimeouts() {
    return timeouts;
  }

  public CustomTagMappings getCustomTagMappings() {
    return tagMappings;
  }

  public NetworkRules getNetworkRules() {
    return networkRules;
  }

  public AmazonEC2ClientProvider getAmazonEc2ClientProvider() {
    return ec2ClientProvider;
  }

  public AmazonEC2AsyncClient getAmazonEc2Client() {
    return ec2Client;
  }

  public AmazonIdentityManagementClientProvider getAmazonIdentityManagementClientProvider() {
    return iamClientProvider;
  }

  public AWSKMSClientProvider getAwsKmsClientProvider() {
    return kmsClientProvider;
  }

  public LocalizationContext getLocalizationContext() {
    return localizationContext;
  }
}
