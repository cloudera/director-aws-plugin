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

import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProviderProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.ACCESS_KEY_ID;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProviderProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.SECRET_ACCESS_KEY;
import static com.cloudera.director.aws.AWSLauncher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.ec2.EC2Provider;
import com.cloudera.director.aws.ec2.EphemeralDeviceMappings;
import com.cloudera.director.aws.ec2.VirtualizationMappings;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.aws.rds.RDSEncryptionInstanceClasses;
import com.cloudera.director.aws.rds.RDSEndpoints;
import com.cloudera.director.aws.rds.RDSProvider;
import com.cloudera.director.spi.v1.common.http.HttpProxyParameters;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.provider.CloudProviderMetadata;
import com.cloudera.director.spi.v1.provider.CredentialsProviderMetadata;
import com.cloudera.director.spi.v1.provider.ResourceProvider;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class AWSProviderTest {

  @Test
  public void testProvider() throws InterruptedException {

    String liveString = System.getProperty("test.aws.live");
    boolean live = Boolean.parseBoolean(liveString);

    CloudProviderMetadata awsProviderMetadata = AWSProvider.METADATA;

    CredentialsProviderMetadata credentialsProviderMetadata =
        awsProviderMetadata.getCredentialsProviderMetadata();
    List<ConfigurationProperty> credentialsConfigurationProperties =
        credentialsProviderMetadata.getCredentialsConfigurationProperties();
    assertTrue(credentialsConfigurationProperties.contains(ACCESS_KEY_ID.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(SECRET_ACCESS_KEY.unwrap()));

    Map<String, String> environmentConfig = Maps.newHashMap();

    // Configure ephemeral device mappings
    EphemeralDeviceMappings ephemeralDeviceMappings =
        EphemeralDeviceMappings.getTestInstance(ImmutableMap.of("m3.large", 1),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure EBS metadata
    EBSMetadata ebsMetadata =
        EBSMetadata.getDefaultInstance(ImmutableMap.of("st1", "500-16384"),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure virtualization mappings
    VirtualizationMappings virtualizationMappings =
        VirtualizationMappings.getTestInstance(ImmutableMap.of("hvm", Arrays.asList("m3.large")),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure RDS endpoints
    RDSEndpoints rdsEndpoints =
        RDSEndpoints.getTestInstance(ImmutableMap.of("us-east-1", "https://rds.us-east-1.amazonaws.com"),
            DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure RDS encryption instance classes
    RDSEncryptionInstanceClasses rdsEncryptionInstanceClasses =
        RDSEncryptionInstanceClasses.getTestInstance(ImmutableList.of("db.m3.large"),
                                                     DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure filters and timeouts
    AWSFilters awsFilters = AWSFilters.EMPTY_FILTERS;
    AWSTimeouts awsTimeouts = new AWSTimeouts(null);

    // Configure custom tag mappings
    CustomTagMappings customTagMappings = new CustomTagMappings(null);

    HttpProxyParameters httpProxyParameters = new HttpProxyParameters();

    LocalizationContext cloudLocalizationContext =
        AWSProvider.METADATA.getLocalizationContext(DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);
    AWSClientConfig awsClientConfig = new AWSClientConfig(new SimpleConfiguration(),
        httpProxyParameters, cloudLocalizationContext);
    Configured configuration = new SimpleConfiguration(environmentConfig);
    AWSProvider awsProvider = new AWSProvider(configuration, ephemeralDeviceMappings, ebsMetadata,
        virtualizationMappings, rdsEndpoints,
        rdsEncryptionInstanceClasses, awsClientConfig,
        awsFilters, awsTimeouts, customTagMappings, NetworkRules.EMPTY_RULES,
        AWSProvider.getCredentialsProvider(configuration, cloudLocalizationContext),
        cloudLocalizationContext);
    assertSame(awsProviderMetadata, awsProvider.getProviderMetadata());

    ResourceProviderMetadata rdsResourceProviderMetadata = null;
    ResourceProviderMetadata ec2ResourceProviderMetadata = null;
    List<ResourceProviderMetadata> resourceProviderMetadatas =
        awsProviderMetadata.getResourceProviderMetadata();
    for (ResourceProviderMetadata resourceProviderMetadata : resourceProviderMetadatas) {
      String resourceProviderId = resourceProviderMetadata.getId();
      if (RDSProvider.ID.equals(resourceProviderId)) {
        rdsResourceProviderMetadata = resourceProviderMetadata;
      } else if (EC2Provider.ID.equals(resourceProviderId)) {
        ec2ResourceProviderMetadata = resourceProviderMetadata;
      } else {
        throw new IllegalArgumentException("Unexpected resource provider: " + resourceProviderId);
      }
    }
    assertNotNull(rdsResourceProviderMetadata);
    assertNotNull(ec2ResourceProviderMetadata);

    // TODO change behavior to use profiles to distinguish live tests
    if (live) {
      ResourceProvider<?, ?> rdsResourceProvider =
          awsProvider.createResourceProvider(RDSProvider.ID,
              new SimpleConfiguration(Collections.<String, String>emptyMap()));
      assertEquals(RDSProvider.class, rdsResourceProvider.getClass());

      ResourceProvider<?, ?> ec2ResourceProvider =
          awsProvider.createResourceProvider(EC2Provider.ID,
              new SimpleConfiguration(Collections.<String, String>emptyMap()));
      assertEquals(EC2Provider.class, ec2ResourceProvider.getClass());
    }
  }
}
