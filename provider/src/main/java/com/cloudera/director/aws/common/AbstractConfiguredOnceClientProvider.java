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

import static java.util.Objects.requireNonNull;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;

import java.util.Objects;

/**
 * A simple implementation for objects which need to be configured once.
 */
public abstract class AbstractConfiguredOnceClientProvider<T> implements ClientProvider<T> {
  protected final AWSCredentialsProvider awsCredentialsProvider;
  protected final ClientConfiguration clientConfiguration;

  private boolean initialized = false;
  private Configured configuration;
  private T instance;

  /**
   * Creates an abstract configured once client provider with the specified parameters.
   *
   * @param awsCredentialsProvider the AWS credentials provider
   * @param clientConfiguration    the client configuration
   */
  protected AbstractConfiguredOnceClientProvider(
      AWSCredentialsProvider awsCredentialsProvider,
      ClientConfiguration clientConfiguration) {
    this.awsCredentialsProvider = requireNonNull(awsCredentialsProvider, "awsCredentialsProvider is null");
    this.clientConfiguration = requireNonNull(clientConfiguration, "clientConfiguration is null");
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  synchronized public T getClient(
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext providerLocalizationContext,
      boolean verify) {

    if (!initialized) {
      instance = requireNonNull(
          doConfigure(configuration, accumulator, providerLocalizationContext, verify),
          "client is null");
      this.configuration = configuration;
      initialized = true;

    } else {
      if (!isEquals(configuration, this.configuration)) {
        throw new IllegalStateException("invariance violation: configuration immutable but changed");
      }
    }

    return instance;
  }

  /**
   * Configures the underlying object.
   * In this method, subclass can do some expensive verification,
   * {@linkplain #getClient(Configured, PluginExceptionConditionAccumulator, LocalizationContext, boolean)}
   * will make sure that the verification is only done once.
   *
   * @param configuration               the provider configuration
   * @param accumulator                 the exception accumulator
   * @param providerLocalizationContext the resource provider localization context
   * @param verify                      whether to verify the configuration by making an API call
   * @return the configured client
   */
  abstract protected T doConfigure(
      Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext providerLocalizationContext,
      boolean verify);

  private static boolean isEquals(Configured lhs, Configured rhs) {
    return Objects.equals(lhs, rhs)
        || lhs != null && rhs != null && Objects.equals(lhs.getConfiguration(null), rhs.getConfiguration(null));
  }
}
