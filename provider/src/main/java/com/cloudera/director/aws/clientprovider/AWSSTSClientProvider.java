// (c) Copyright 2019 Cloudera, Inc.
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

import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.STS_REGION_ENDPOINT;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClient;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS STS client provider.
 * <p/>
 * The class verifies the connectivity once in its scope, which means that if the credential provider
 * is instance profile credential provider, and user has changed the role policy, the time to pick
 * up the credential change is the max of refresh method of credential provider and the scope of provider.
 */
public class AWSSTSClientProvider extends AbstractConfiguredOnceClientProvider<AWSSecurityTokenServiceAsyncClient> {
  private static final Logger LOG = LoggerFactory.getLogger(AWSSTSClientProvider.class);

  /**
   * Creates an Amazon security token client provider with the specified parameters.
   *
   * @param awsCredentialsProvider the AWS credentials provider
   * @param clientConfiguration    the client configuration
   */
  public AWSSTSClientProvider(
      AWSCredentialsProvider awsCredentialsProvider,
      ClientConfiguration clientConfiguration) {
    super(awsCredentialsProvider, clientConfiguration);
  }

  @Override
  protected AWSSecurityTokenServiceAsyncClient doConfigure(Configured configuration, PluginExceptionConditionAccumulator accumulator, LocalizationContext providerLocalizationContext, boolean verify) {

    AWSSecurityTokenServiceAsyncClient client =
        new AWSSecurityTokenServiceAsyncClient(awsCredentialsProvider, clientConfiguration);

    try {
      String regionEndpoint = configuration.getConfigurationValue(STS_REGION_ENDPOINT, providerLocalizationContext);

      // We only configure the region endpoint if it's specifically set, as AWS uses a global endpoint by default.
      // Other regions/endpoints must be explicitly enabled by the user.
      if (regionEndpoint != null) {
        LOG.info("<< Using configured region endpoint for STS client: {}", regionEndpoint);
        client.setEndpoint(regionEndpoint);
      }
    } catch (AmazonClientException e) {
      throw AWSExceptions.propagate(client, e);
    }

    return client;
  }
}
