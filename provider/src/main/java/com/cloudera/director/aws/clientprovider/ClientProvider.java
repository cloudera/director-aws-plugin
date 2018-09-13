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

import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;

/**
 * Client provider.
 *
 * @param <T> type of client
 */
public interface ClientProvider<T> {

  /**
   * Returns a configured client.
   *
   * @param configuration               the provider configuration
   * @param accumulator                 the exception accumulator
   * @param providerLocalizationContext the resource provider localization context
   * @param verify                      whether to verify the configuration by making an API call
   * @return the configured client
   */
  T getClient(
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext providerLocalizationContext,
      boolean verify);
}
