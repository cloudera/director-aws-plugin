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

package com.cloudera.director.aws.common;

import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION_ENDPOINT;
import static java.util.Objects.requireNonNull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.amazonaws.services.ec2.model.Region;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Amazon EC2 Client provider.
 * <p/>
 * The class verifies the connectivity once in its scope, which means that if the credential provider
 * is instance profile credential provider, and user has changed the role policy, the time to pick
 * up the credential change is the max of refresh method of credential provider and the scope of provider.
 */
public class AmazonEC2ClientProvider
    extends AbstractConfiguredOnceClientProvider<AmazonEC2AsyncClient> {
  private static final Logger LOG = LoggerFactory.getLogger(AmazonEC2ClientProvider.class);

  /**
   * Creates an Amazon EC2 client provider with the specified parameters.
   *
   * @param awsCredentialsProvider the AWS credentials provider
   * @param clientConfiguration    the client configuration
   */
  public AmazonEC2ClientProvider(
      AWSCredentialsProvider awsCredentialsProvider,
      ClientConfiguration clientConfiguration) {
    super(awsCredentialsProvider, clientConfiguration);
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  protected AmazonEC2AsyncClient doConfigure(
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext providerLocalizationContext,
      boolean verify) {

    AmazonEC2AsyncClient client = new AmazonEC2AsyncClient(awsCredentialsProvider, clientConfiguration);

    try {
      String regionEndpoint =
          configuration.getConfigurationValue(REGION_ENDPOINT, providerLocalizationContext);
      if (regionEndpoint != null) {
        LOG.info("<< Using configured region endpoint for EC2 client: {}", regionEndpoint);
      } else {
        String region = configuration.getConfigurationValue(REGION, providerLocalizationContext);
        regionEndpoint = getEndpointForRegion(client, region);
        verify = false;
      }
      client.setEndpoint(regionEndpoint);

      if (verify) {
        // Attempt to use client, to validate credentials and connectivity
        client.describeRegions();
      }
    } catch (AmazonClientException e) {
      throw AWSExceptions.propagate(e);
    } catch (IllegalArgumentException e) {
      accumulator.addError(REGION.unwrap().getConfigKey(), e.getMessage());
    }

    return client;
  }

  private static String getEndpointForRegion(AmazonEC2Client client, String regionName) {
    requireNonNull(client, "client is null");
    requireNonNull(regionName, "regionName is null");

    LOG.info(">> Describing all regions to find endpoint for '{}'", regionName);

    DescribeRegionsResult result = client.describeRegions();
    List<String> regions = Lists.newArrayListWithExpectedSize(result.getRegions().size());

    for (Region candidate : result.getRegions()) {
      regions.add(candidate.getRegionName());

      if (candidate.getRegionName().equals(regionName)) {
        LOG.info("<< Found endpoint '{}' for region '{}'", candidate.getEndpoint(), regionName);

        return candidate.getEndpoint();
      }
    }

    throw new IllegalArgumentException(String.format("Unable to find an endpoint for region '%s'. "
        + "Choose one of the following regions: %s", regionName, Joiner.on(", ").join(regions)));
  }
}
