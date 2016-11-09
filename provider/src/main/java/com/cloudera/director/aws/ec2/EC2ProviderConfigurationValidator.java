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

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.kms.AWSKMSClient;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;

/**
 * Validates EC2 provider configuration.
 */
public class EC2ProviderConfigurationValidator implements ConfigurationValidator {

  /**
   * The EC2 client.
   */
  private final AmazonEC2Client client;

  /**
   * The IAM client.
   */
  private final AmazonIdentityManagementClient identityManagementClient;

  /**
   * The KMS client.
   */
  private final AWSKMSClient kmsClient;

  /**
   * Creates an EC2 provider configuration validator with the specified parameters.
   *
   * @param client the EC2 client
   * @param identityManagementClient the IAM client
   * @param kmsClient the KMS client
   */
  public EC2ProviderConfigurationValidator(AmazonEC2Client client,
                                           AmazonIdentityManagementClient identityManagementClient,
                                           AWSKMSClient kmsClient) {
    this.client = checkNotNull(client, "client is null");
    this.identityManagementClient = checkNotNull(identityManagementClient,
                                                 "identityManagementClient is null");
    this.kmsClient = checkNotNull(kmsClient, "kmsClient is null");
  }

  @Override
  public void validate(String name, Configured configuration,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {
    EC2Provider.configureClient(configuration, accumulator, client, localizationContext, true);
    EC2Provider.configureIAMClient(configuration, accumulator, identityManagementClient,
                                   localizationContext, true);
    EC2Provider.configureKmsClient(configuration, accumulator, kmsClient, localizationContext);
  }
}
