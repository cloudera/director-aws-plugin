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

package com.cloudera.director.aws.clientprovider;

import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.KMS_REGION_ENDPOINT;
import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.REGION;
import static java.util.Objects.requireNonNull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClient;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS KMS client provider.
 * <p/>
 * The class verifies the connectivity once in its scope, which means that if the credential provider
 * is instance profile credential provider, and user has changed the role policy, the time to pick
 * up the credential change is the max of refresh method of credential provider and the scope of provider.
 */
public class AWSKMSClientProvider extends AbstractConfiguredOnceClientProvider<AWSKMSClient> {
  private static final Logger LOG = LoggerFactory.getLogger(AWSKMSClientProvider.class);

  /**
   * Creates an Amazon KMS client provider with the specified parameters.
   *
   * @param awsCredentialsProvider the AWS credentials provider
   * @param clientConfiguration    the client configuration
   */
  public AWSKMSClientProvider(
      AWSCredentialsProvider awsCredentialsProvider,
      ClientConfiguration clientConfiguration) {
    super(awsCredentialsProvider, clientConfiguration);
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  protected AWSKMSClient doConfigure(
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext providerLocalizationContext,
      boolean verify) {

    AWSKMSClient client = new AWSKMSClient(awsCredentialsProvider, clientConfiguration);

    try {
      String regionEndpoint = configuration.getConfigurationValue(KMS_REGION_ENDPOINT, providerLocalizationContext);
      if (regionEndpoint != null) {
        LOG.info("<< Using configured region endpoint for KMS client: {}", regionEndpoint);
      } else {
        String region = configuration.getConfigurationValue(REGION, providerLocalizationContext);
        regionEndpoint = getKMSEndpointForRegion(client, region);
      }
      client.setEndpoint(regionEndpoint);
    } catch (AmazonClientException e) {
      AWSSecurityTokenServiceAsyncClient stsClient = new AWSSTSClientProvider(awsCredentialsProvider,
          clientConfiguration) .doConfigure(configuration, accumulator, providerLocalizationContext, verify);
      throw AWSExceptions.propagate(stsClient, e);
    } catch (IllegalArgumentException e) {
      accumulator.addError(REGION.unwrap().getConfigKey(), e.getMessage());
    }

    return client;
  }

  /**
   * Returns the KMS endpoint URL for the specified region.
   *
   * @param kmsClient  the KMS client
   * @param regionName the desired region
   * @return the endpoint URL for the specified region
   * @throws IllegalArgumentException if the endpoint cannot be determined
   */
  private static String getKMSEndpointForRegion(AWSKMSClient kmsClient, String regionName) {
    requireNonNull(kmsClient, "kmsClient is null");
    requireNonNull(regionName, "regionName is null");

    com.amazonaws.regions.Region region = RegionUtils.getRegion(regionName);

    if (region == null) {
      throw new IllegalArgumentException(String.format("Unable to find the region %s", regionName));
    }

    String serviceName = kmsClient.getServiceName();
    String protocolPrefix = region.hasHttpsEndpoint(serviceName) ? "https://" : "http://";
    return protocolPrefix + region.getServiceEndpoint(serviceName);
  }
}
