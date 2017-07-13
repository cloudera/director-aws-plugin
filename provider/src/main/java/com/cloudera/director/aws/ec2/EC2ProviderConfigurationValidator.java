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

package com.cloudera.director.aws.ec2;

import static java.util.Objects.requireNonNull;

import com.cloudera.director.aws.common.AWSKMSClientProvider;
import com.cloudera.director.aws.common.AmazonEC2ClientProvider;
import com.cloudera.director.aws.common.AmazonIdentityManagementClientProvider;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;

/**
 * Validates EC2 provider configuration.
 */
public class EC2ProviderConfigurationValidator implements ConfigurationValidator {

  /**
   * The EC2 client provider.
   */
  private final AmazonEC2ClientProvider clientProvider;

  /**
   * The IAM client provider.
   */
  private final AmazonIdentityManagementClientProvider identityManagementClientProvider;

  /**
   * The KMS client provider.
   */
  private final AWSKMSClientProvider kmsClientProvider;

  /**
   * Creates an EC2 provider configuration validator with the specified parameters.
   *
   * @param clientProvider                   the EC2 client provider
   * @param identityManagementClientProvider the IAM client provider
   * @param kmsClientProvider                the KMS client provider
   */
  public EC2ProviderConfigurationValidator(
      AmazonEC2ClientProvider clientProvider,
      AmazonIdentityManagementClientProvider identityManagementClientProvider,
      AWSKMSClientProvider kmsClientProvider) {
    this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
    this.identityManagementClientProvider = requireNonNull(
        identityManagementClientProvider, "identityManagementClientProvider is null");
    this.kmsClientProvider = requireNonNull(kmsClientProvider, "kmsClientProvider is null");
  }

  @Override
  public void validate(String name, Configured configuration,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {

    clientProvider.getClient(configuration, accumulator, localizationContext, true);
    identityManagementClientProvider.getClient(configuration, accumulator, localizationContext, true);
    kmsClientProvider.getClient(configuration, accumulator, localizationContext, true);
  }
}
