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
import com.cloudera.director.spi.v1.common.http.HttpProxyParameters;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;

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

  /**
   * AWS client configuration properties.
   *
   * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html">ClientConfiguration</a>
   */
  // Fully qualifying class name due to compiler bug
  public static enum AWSClientConfigurationPropertyToken
      implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

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
    private AWSClientConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
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

  /**
   * Returns an AWS ClientConfiguration representing the current proxy state.
   *
   * @return An AWS ClientConfiguration
   */
  public ClientConfiguration getClientConfiguration() {
    return new ClientConfiguration()
        .withMaxErrorRetry(getMaxErrorRetries())
        .withConnectionTimeout(getConnectionTimeoutInMilliseconds())
        .withProxyHost(httpProxyParameters.getHost())
        .withProxyPort(httpProxyParameters.getPort())
        .withProxyUsername(httpProxyParameters.getUsername())
        .withProxyPassword(httpProxyParameters.getPassword())
        .withProxyDomain(httpProxyParameters.getDomain())
        .withProxyWorkstation(httpProxyParameters.getWorkstation())
        .withPreemptiveBasicProxyAuth(httpProxyParameters.isPreemptiveBasicProxyAuth());
  }
}
