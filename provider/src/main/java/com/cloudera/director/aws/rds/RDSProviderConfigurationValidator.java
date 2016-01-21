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

package com.cloudera.director.aws.rds;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.rds.AmazonRDSClient;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;

/**
 * Validates RDS provider configuration.
 */
public class RDSProviderConfigurationValidator implements ConfigurationValidator {

  /**
   * The RDS client.
   */
  private final AmazonRDSClient client;

  /**
   * The RDS endpoints.
   */
  private final RDSEndpoints endpoints;

  /**
   * Creates an RDS provider configuration validator with the specified parameters.
   *
   * @param client    the RDS client
   * @param endpoints the RDS endpoints
   */
  public RDSProviderConfigurationValidator(AmazonRDSClient client, RDSEndpoints endpoints) {
    this.client = checkNotNull(client, "client is null");
    this.endpoints = checkNotNull(endpoints, "endpoints is null");
  }

  @Override
  public void validate(String name, Configured configuration,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {
    RDSProvider.configureClient(configuration, accumulator, client, endpoints, localizationContext, true);
  }
}
