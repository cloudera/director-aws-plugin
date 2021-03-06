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

package com.cloudera.director.aws.rds.provider;

import static java.util.Objects.requireNonNull;

import com.amazonaws.services.rds.AmazonRDSClient;
import com.cloudera.director.aws.clientprovider.ClientProvider;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;

/**
 * Validates RDS provider configuration.
 */
public class RDSProviderConfigurationValidator implements ConfigurationValidator {

  /**
   * The RDS client provider.
   */
  private final ClientProvider<AmazonRDSClient> clientProvider;

  /**
   * Creates an RDS provider configuration validator with the specified parameters.
   *
   * @param clientProvider the RDS client provider
   */
  public RDSProviderConfigurationValidator(ClientProvider<AmazonRDSClient> clientProvider) {
    this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
  }

  @Override
  public void validate(String name, Configured configuration,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {
    clientProvider.getClient(configuration, accumulator, localizationContext, true);
  }
}
