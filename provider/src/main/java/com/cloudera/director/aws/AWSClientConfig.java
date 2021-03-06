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

package com.cloudera.director.aws;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.cloudera.director.spi.v2.common.http.HttpProxyParameters;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.Property;
import com.cloudera.director.spi.v2.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common configuration options across AWS clients.
 */
public class AWSClientConfig {

  private static final Logger LOG = LoggerFactory.getLogger(AWSClientConfig.class);

  // The default backoff strategy is exponential + jitter with a 100ms delay:
  // jitter(min(2^retryNumber * 100, 20000) milliseconds. Worst case should be 165.5 seconds assuming the jitter
  // always picks the maximum possible wait time.
  // For throttling issues, a larger delay is applied (500ms) on top of this, so:
  // jitter(min(2^retryNumber * 500, 20000) milliseconds. Worse case should be 211.5 seconds assuming the jitter
  // always picks the maximum possible wait time.
  public static final int DEFAULT_MAX_ERROR_RETRIES = 15;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10000;
  public static final ClientConfiguration DEFAULT_CLIENT_CONFIG = new ClientConfiguration()
      .withMaxErrorRetry(DEFAULT_MAX_ERROR_RETRIES)
      .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT_MILLIS);

  // Custom retry policy that will log when retry attempts by the AWS client
  private static final boolean DEFAULT_LOG_RETRY_ATTEMPTS = false;
  private static final boolean HONOR_MAX_ERROR_RETRY_IN_CLIENT_CONFIG = true;
  private static final RetryPolicy RETRY_POLICY_WITH_LOGGING = new RetryPolicy(
      PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
      new DefaultBackoffStrategyWithLogging(),
      PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY,
      HONOR_MAX_ERROR_RETRY_IN_CLIENT_CONFIG);

  /**
   * AWS client configuration properties.
   *
   * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html">ClientConfiguration</a>
   */
  // Fully qualifying class name due to compiler bug
  public enum AWSClientConfigurationPropertyToken
      implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

    /**
     * The maximum number of retry attempts for failed retryable requests.
     */
    MAX_ERROR_RETRIES(new SimpleConfigurationPropertyBuilder()
        .configKey("maxErrorRetries")
        .name("Maximum retries")
        .defaultDescription("The maximum number of retry attempts for failed retryable requests.")
        .build()) {
      @Override
      protected void setFieldValue(AWSClientConfig clientConfig, String propertyValue) {
        clientConfig.setMaxErrorRetries(Integer.parseInt(propertyValue));
      }
    },

    /**
     * The amount of time to wait (in milliseconds) when initially establishing a connection before
     * giving up and timing out.
     */
    CONNECTION_TIMEOUT_IN_MILLISECONDS(new SimpleConfigurationPropertyBuilder()
        .configKey("connectionTimeoutInMilliseconds")
        .name("Connection timeout (ms)")
        .defaultDescription("The amount of time to wait (in milliseconds) when initially"
            + " establishing a connection before giving up and timing out.")
        .build()) {
      @Override
      protected void setFieldValue(AWSClientConfig clientConfig, String propertyValue) {
        clientConfig.setConnectionTimeoutInMilliseconds(Integer.parseInt(propertyValue));
      }
    },

    /**
     * Whether to log internal AWS client retries.
     */
    LOG_RETRY_ATTEMPTS(new SimpleConfigurationPropertyBuilder()
        .configKey("logRetryAttempts")
        .name("Log client retries")
        .type(Property.Type.BOOLEAN)
        .defaultDescription("Whether to log AWS client retries.")
        .build()) {
      @Override
      protected void setFieldValue(AWSClientConfig clientConfig, String propertyValue) {
        clientConfig.setLogRetryAttempts(Boolean.parseBoolean(propertyValue));
      }
    };

    /**
     * The configuration property.
     */
    private final ConfigurationProperty configurationProperty;

    /**
     * Creates a configuration property token with the specified parameters.
     *
     * @param configurationProperty the configuration property
     */
    AWSClientConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    /**
     * Returns the configuration property.
     *
     * @return the configuration property
     */
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }

    /**
     * Reads the property from the specified configuration, and if it is present, sets the
     * corresponding field on the AWS client config.
     *
     * @param configuration       the configuration
     * @param properties          the AWS client config
     * @param localizationContext the localization context
     */
    private void readAndSetPropertyValue(Configured configuration, AWSClientConfig properties,
        LocalizationContext localizationContext) {
      String propertyValue = configuration.getConfigurationValue(unwrap(), localizationContext);
      if (propertyValue != null) {
        setFieldValue(properties, propertyValue);
      }
    }

    /**
     * Sets the value of the corresponding field on the AWS client config using the specified value.
     *
     * @param clientConfig  the AWS client config
     * @param propertyValue the property value, which is known to be non-{@code null}
     */
    protected abstract void setFieldValue(AWSClientConfig clientConfig, String propertyValue);
  }

  private int maxErrorRetries = DEFAULT_MAX_ERROR_RETRIES;
  private int connectionTimeoutInMilliseconds = DEFAULT_CONNECTION_TIMEOUT_MILLIS;
  private boolean logRetryAttempts = DEFAULT_LOG_RETRY_ATTEMPTS;
  private HttpProxyParameters httpProxyParameters;

  /**
   * Creates AWS client config with default configuration.
   */
  public AWSClientConfig() {
    httpProxyParameters = new HttpProxyParameters();
  }

  /**
   * Creates AWS client config with the specified configuration.
   *
   * @param configuration             the configuration
   * @param httpProxyParameters       the HTTP proxy parameters
   * @param parentLocalizationContext the parent localization context
   */
  public AWSClientConfig(Configured configuration,
      HttpProxyParameters httpProxyParameters,
      LocalizationContext parentLocalizationContext) {
    checkNotNull(configuration, "configuration is null");
    setHttpProxyParameters(httpProxyParameters);
    LocalizationContext localizationContext =
        new ChildLocalizationContext(parentLocalizationContext, "client");
    for (AWSClientConfigurationPropertyToken propertyToken : AWSClientConfigurationPropertyToken.values()) {
      propertyToken.readAndSetPropertyValue(configuration, this, localizationContext);
    }
  }

  public int getMaxErrorRetries() {
    return maxErrorRetries;
  }

  public void setMaxErrorRetries(int maxErrorRetries) {
    LOG.info("Overriding maxErrorRetries={} (default {})", maxErrorRetries,
        DEFAULT_MAX_ERROR_RETRIES);
    this.maxErrorRetries = maxErrorRetries;
  }

  public int getConnectionTimeoutInMilliseconds() {
    return connectionTimeoutInMilliseconds;
  }

  public void setConnectionTimeoutInMilliseconds(int connectionTimeoutInMilliseconds) {
    LOG.info("Overriding connectionTimeoutInMilliseconds={} (default {})",
        connectionTimeoutInMilliseconds, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
    this.connectionTimeoutInMilliseconds = connectionTimeoutInMilliseconds;
  }

  public void setHttpProxyParameters(HttpProxyParameters httpProxyParameters) {
    this.httpProxyParameters = checkNotNull(httpProxyParameters, "httpProxyParameters is null");
  }

  public boolean isLogRetryAttempts() {
    return logRetryAttempts;
  }

  public void setLogRetryAttempts(boolean logRetryAttempts) {
    LOG.info("Overriding logRetryAttempts={} (default {})", logRetryAttempts,
        DEFAULT_LOG_RETRY_ATTEMPTS);
    this.logRetryAttempts = logRetryAttempts;
  }

  /**
   * Returns an AWS ClientConfiguration representing the current proxy state.
   *
   * @return An AWS ClientConfiguration
   */
  public ClientConfiguration getClientConfiguration() {
    ClientConfiguration clientConfig = new ClientConfiguration()
        .withMaxErrorRetry(getMaxErrorRetries())
        .withConnectionTimeout(getConnectionTimeoutInMilliseconds())
        .withProxyHost(httpProxyParameters.getHost())
        .withProxyPort(httpProxyParameters.getPort())
        .withProxyUsername(httpProxyParameters.getUsername())
        .withProxyPassword(httpProxyParameters.getPassword())
        .withProxyDomain(httpProxyParameters.getDomain())
        .withProxyWorkstation(httpProxyParameters.getWorkstation())
        .withPreemptiveBasicProxyAuth(httpProxyParameters.isPreemptiveBasicProxyAuth())
        .withNonProxyHosts(modifyProxyBypassList(httpProxyParameters.getProxyBypassHosts()));

    if (isLogRetryAttempts()) {
      clientConfig.withRetryPolicy(RETRY_POLICY_WITH_LOGGING);
    }

    return clientConfig;
  }

  /**
   * Modify the proxy bypass host list so that it can be used by AWS's client configuration object. The
   * list will be modified in accordance with the documentation for http.nonProxyHosts on
   * http://docs.oracle.com/javase/7/docs/api/java/net/doc-files/net-properties.html, which is supported by
   * the AWS EC2 SDK.
   *
   * @param proxyBypassHosts the list of proxyBypassHosts to analyze
   * @return a modified string of proxy bypass hosts
   */
  @VisibleForTesting
  static String modifyProxyBypassList(List<String> proxyBypassHosts) {
    List<String> nonProxyHosts = Lists.newArrayListWithCapacity(proxyBypassHosts.size());

    for (String proxyBypassHost : proxyBypassHosts) {
      if (proxyBypassHost.startsWith(".")) {
        nonProxyHosts.add("*" + proxyBypassHost);
      } else {
        nonProxyHosts.add(proxyBypassHost);
      }
    }

    return Joiner.on('|').join(nonProxyHosts);
  }
}
