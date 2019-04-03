// (c) Copyright 2017 Cloudera, Inc.
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

package com.cloudera.director.aws.ec2.provider;

import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.IMPORT_KEY_PAIR_IF_MISSING;
import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.KEY_NAME_PREFIX;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.clientprovider.ClientProvider;
import com.cloudera.director.aws.ec2.EphemeralDeviceMappings;
import com.cloudera.director.aws.ec2.VirtualizationMappings;
import com.cloudera.director.aws.ec2.ebs.EBSDeviceMappings;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.shaded.com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.kms.AWSKMSClient;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClient;
import com.cloudera.director.aws.shaded.com.google.common.base.Optional;
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
  private ClientProvider<AmazonEC2AsyncClient> ec2ClientProvider;
  private ClientProvider<AmazonAutoScalingAsyncClient> autoScalingClientProvider;
  private ClientProvider<AmazonIdentityManagementClient> iamClientProvider;
  private ClientProvider<AWSKMSClient> kmsClientProvider;
  private ClientProvider<AWSSecurityTokenServiceAsyncClient> stsClientProvider;
  private LocalizationContext localizationContext;

  private AWSFilters ec2Filters;
  private AWSFilters templateFilters;

  private AmazonEC2AsyncClient ec2Client;
  private AmazonAutoScalingAsyncClient autoScalingClient;

  @SuppressWarnings("unchecked")
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
    ec2ClientProvider = mock(ClientProvider.class);
    autoScalingClientProvider = mock(ClientProvider.class);
    iamClientProvider = mock(ClientProvider.class);
    kmsClientProvider = mock(ClientProvider.class);
    stsClientProvider = mock(ClientProvider.class);
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

    autoScalingClient = mock(AmazonAutoScalingAsyncClient.class);
    when(autoScalingClientProvider.getClient(eq(configured), any(PluginExceptionConditionAccumulator.class),
        any(LocalizationContext.class), eq(false)))
        .thenReturn(autoScalingClient);

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
                           ec2ClientProvider, autoScalingClientProvider, iamClientProvider,
                           kmsClientProvider, stsClientProvider, true, localizationContext);
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

  public ClientProvider<AmazonEC2AsyncClient> getAmazonEc2ClientProvider() {
    return ec2ClientProvider;
  }

  public AmazonEC2AsyncClient getAmazonEc2Client() {
    return ec2Client;
  }

  public AmazonAutoScalingAsyncClient getAmazonAutoScalingClient() {
    return autoScalingClient;
  }

  public ClientProvider<AmazonIdentityManagementClient> getAmazonIdentityManagementClientProvider() {
    return iamClientProvider;
  }

  public ClientProvider<AWSKMSClient> getAwsKmsClientProvider() {
    return kmsClientProvider;
  }

  public LocalizationContext getLocalizationContext() {
    return localizationContext;
  }
}
