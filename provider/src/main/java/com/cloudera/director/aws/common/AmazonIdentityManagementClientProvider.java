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

import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.IAM_ENDPOINT;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Amazon identity management client provider.
 * <p/>
 * The class verifies the connectivity once in its scope, which means that if the credential provider
 * is instance profile credential provider, and user has changed the role policy, the time to pick
 * up the credential change is the max of refresh method of credential provider and the scope of provider.
 */
public class AmazonIdentityManagementClientProvider
    extends AbstractConfiguredOnceClientProvider<AmazonIdentityManagementClient> {
  private static final Logger LOG = LoggerFactory.getLogger(AmazonIdentityManagementClientProvider.class);

  /**
   * Creates an Amazon identity management client provider with the specified parameters.
   *
   * @param awsCredentialsProvider the AWS credentials provider
   * @param clientConfiguration    the client configuration
   */
  public AmazonIdentityManagementClientProvider(
      AWSCredentialsProvider awsCredentialsProvider,
      ClientConfiguration clientConfiguration) {
    super(awsCredentialsProvider, clientConfiguration);
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  protected AmazonIdentityManagementClient doConfigure(
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext providerLocalizationContext,
      boolean verify) {

    AmazonIdentityManagementClient client = new AmazonIdentityManagementClient(
        awsCredentialsProvider, clientConfiguration);

    try {
      String iamEndpoint =
          configuration.getConfigurationValue(IAM_ENDPOINT, providerLocalizationContext);
      if (iamEndpoint != null) {
        LOG.info("<< Using configured IAM endpoint: {}", iamEndpoint);
        client.setEndpoint(iamEndpoint);
      }
      // else use the single default endpoint for all of AWS (outside GovCloud)

      if (verify) {
        // Attempt to use client, to validate credentials and connectivity
        try {
          client.getInstanceProfile(new GetInstanceProfileRequest().withInstanceProfileName("test"));
        } catch (NoSuchEntityException e) {
        /* call succeeded */
        }
      }

    } catch (AmazonClientException e) {
      throw AWSExceptions.propagate(e);
    } catch (IllegalArgumentException e) {
      accumulator.addError(IAM_ENDPOINT.unwrap().getConfigKey(), e.getMessage());
    }

    return client;
  }

  @Override
  protected boolean isEquals(Configured lhs, Configured rhs, LocalizationContext localizationContext) {
    boolean isEqual = super.isEquals(lhs, rhs, localizationContext);
    isEqual = isEqual || isEqualConfigValue(lhs, rhs, localizationContext, IAM_ENDPOINT);
    return isEqual;
  }

  private boolean isEqualConfigValue(Configured lhs, Configured rhs, LocalizationContext localizationContext,
                                     ConfigurationPropertyToken configToken) {
    String lhsValue = lhs.getConfigurationValue(configToken, localizationContext);
    String rhsValue = rhs.getConfigurationValue(configToken, localizationContext);
    return Objects.equals(lhsValue, rhsValue);
  }
}
