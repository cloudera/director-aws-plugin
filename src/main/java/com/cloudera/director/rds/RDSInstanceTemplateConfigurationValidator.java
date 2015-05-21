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

package com.cloudera.director.rds;

import static com.cloudera.director.spi.v1.model.util.Validations.addError;

import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.util.Preconditions;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates RDS instance template configuration.
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "PMD.UnusedPrivateField", "PMD.UnusedPrivateField",
    "unused", "FieldCanBeLocal"})
public class RDSInstanceTemplateConfigurationValidator implements ConfigurationValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDSInstanceTemplateConfigurationValidator.class);

  private static final String INVALID_COUNT_EMPTY_MSG = "%s not found: %s";
  private static final String INVALID_COUNT_DUPLICATES_MSG =
      "More than one %s found with identifier %s";

  /**
   * The RDS provider.
   */
  private final RDSProvider provider;

  /**
   * Creates an RDS instance template configuration validator with the specified parameters.
   *
   * @param provider the RDS provider
   */
  public RDSInstanceTemplateConfigurationValidator(RDSProvider provider) {
    this.provider = Preconditions.checkNotNull(provider, "provider");
  }

  @Override
  public void validate(String name, Configured configuration, PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {

    // TODO add validations
  }

  /**
   * Verifies that the specified result list has exactly one element, and reports an appropriate
   * error otherwise.
   *
   * @param accumulator         the exception condition accumulator
   * @param token               the token representing the configuration property in error
   * @param localizationContext the localization context
   * @param field               the problem field value
   * @param result              the result list to be validated
   */
  private void checkCount(PluginExceptionConditionAccumulator accumulator,
      ConfigurationPropertyToken token, LocalizationContext localizationContext,
      String field, List<?> result) {

    if (result.isEmpty()) {
      addError(accumulator, token, localizationContext,
          INVALID_COUNT_EMPTY_MSG, token.unwrap().getName(localizationContext), field);
    }

    if (result.size() > 1) {
      addError(accumulator, token, localizationContext,
          INVALID_COUNT_DUPLICATES_MSG, token.unwrap().getName(localizationContext), field);
    }
  }
}
