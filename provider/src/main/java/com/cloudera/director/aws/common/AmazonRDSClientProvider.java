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

import static com.cloudera.director.aws.rds.RDSProvider.RDSProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.aws.rds.RDSProvider.RDSProviderConfigurationPropertyToken.REGION_ENDPOINT;
import static java.util.Objects.requireNonNull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.AmazonRDSException;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.ec2.EC2Provider;
import com.cloudera.director.aws.rds.RDSEndpoints;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Amazon RDS client provider.
 * <p/>
 * The class verifies the connectivity once in its scope, which means that if the credential provider
 * is instance profile credential provider, and user has changed the role policy, the time to pick
 * up the credential change is the max of refresh method of credential provider and the scope of provider.
 */
public class AmazonRDSClientProvider extends AbstractConfiguredOnceClientProvider<AmazonRDSClient> {
  /**
   * The key used for accumulating validation warnings or errors related to
   * authorization.
   */
  public static final String RDS_AUTHORIZATION_KEY = "_rdsAuthorization";

  private static final Logger LOG = LoggerFactory.getLogger(AmazonRDSClientProvider.class);

  private final RDSEndpoints rdsEndpoints;

  /**
   * Creates an Amazon RDS client provider with the specified parameters.
   *
   * @param awsCredentialsProvider the AWS credentials provider
   * @param clientConfiguration    the client configuration
   * @param rdsEndpoints           the RDS endpoints
   */
  public AmazonRDSClientProvider(
      AWSCredentialsProvider awsCredentialsProvider,
      ClientConfiguration clientConfiguration,
      RDSEndpoints rdsEndpoints) {
    super(awsCredentialsProvider, clientConfiguration);
    this.rdsEndpoints = requireNonNull(rdsEndpoints, "rdsEndpoints is null");
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  protected AmazonRDSClient doConfigure(
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext providerLocalizationContext,
      boolean verify) {

    AmazonRDSClient client = new AmazonRDSClient(awsCredentialsProvider, clientConfiguration);

    try {
      String regionEndpoint =
          configuration.getConfigurationValue(REGION_ENDPOINT, providerLocalizationContext);
      if (regionEndpoint != null) {
        LOG.info("<< Using configured region endpoint: {}", regionEndpoint);
      } else {
        String region = configuration.getConfigurationValue(REGION, providerLocalizationContext);
        if (region == null) {
          region = configuration.getConfigurationValue(
              EC2Provider.EC2ProviderConfigurationPropertyToken.REGION,
              providerLocalizationContext);
        }
        regionEndpoint = getEndpointForRegion(rdsEndpoints, region);
      }
      client.setEndpoint(regionEndpoint);

      if (verify) {
        // Attempt to use client, to validate credentials and connectivity
        client.describeDBSecurityGroups();
      }

    } catch (AmazonClientException e) {
      if (e instanceof AmazonRDSException && ((AmazonRDSException) e).getStatusCode() == 403) {
        // not authorized for RDS - add a warning
        accumulator.addWarning(RDS_AUTHORIZATION_KEY, e.getMessage());
      } else {
        throw AWSExceptions.propagate(e);
      }

    } catch (IllegalArgumentException e) {
      accumulator.addError(REGION.unwrap().getConfigKey(), e.getMessage());
    }

    return client;
  }

  private static String getEndpointForRegion(RDSEndpoints endpoints, String regionName) {
    requireNonNull(regionName, "regionName is null");
    String endpoint = endpoints.apply(regionName);
    if (endpoint == null) {
      throw new IllegalArgumentException(String.format(
          "Endpoint unknown for region %s. Please configure it as a custom " +
              "RDS endpoint.", regionName));
    }
    return endpoint;
  }
}
