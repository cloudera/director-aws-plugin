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

package com.cloudera.director.config;

import com.cloudera.director.common.http.ProxyParameters;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.ChildLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration options common to all providers
 */
public class HttpProxyConfigProperties {

  private static final Logger LOG = LoggerFactory.getLogger(HttpProxyConfigProperties.class);

  private static final int DEFAULT_PROXY_PORT = -1; // -1 means the port is unset

  /**
   * HTTP proxy configuration properties.
   */
  // Fully qualifying class name due to compiler bug
  public static enum HttpProxyConfigurationPropertyToken
      implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

    /**
     * The proxy host.
     */
    HOST(new SimpleConfigurationPropertyBuilder()
        .configKey("host")
        .name("Host")
        .defaultDescription("The proxy host.")
        .build()) {
      @Override
      protected void setFieldValue(HttpProxyConfigProperties properties, String propertyValue) {
        properties.setHost(propertyValue);
      }
    },

    /**
     * The proxy port.
     */
    PORT(new SimpleConfigurationPropertyBuilder()
        .configKey("port")
        .name("Port")
        .defaultDescription("The proxy port.")
        .build()) {
      @Override
      protected void setFieldValue(HttpProxyConfigProperties properties, String propertyValue) {
        properties.setPort(Integer.parseInt(propertyValue));
      }
    },

    /**
     * The proxy username.
     */
    USERNAME(new SimpleConfigurationPropertyBuilder()
        .configKey("username")
        .name("Username")
        .defaultDescription("The proxy username.")
        .build()) {
      @Override
      protected void setFieldValue(HttpProxyConfigProperties properties, String propertyValue) {
        properties.setUsername(propertyValue);
      }
    },

    /**
     * The proxy password.
     */
    PASSWORD(new SimpleConfigurationPropertyBuilder()
        .configKey("password")
        .name("Password")
        .widget(ConfigurationProperty.Widget.PASSWORD)
        .defaultDescription("The proxy password.")
        .sensitive(true)
        .build()) {
      @Override
      protected void setFieldValue(HttpProxyConfigProperties properties, String propertyValue) {
        properties.setPassword(propertyValue);
      }
    },

    /**
     * The proxy domain (only used in NTLM authentication).
     */
    DOMAIN(new SimpleConfigurationPropertyBuilder()
        .configKey("domain")
        .name("Domain")
        .defaultDescription("The proxy domain (only used in NTLM authentication).")
        .build()) {
      @Override
      protected void setFieldValue(HttpProxyConfigProperties properties, String propertyValue) {
        properties.setDomain(propertyValue);
      }
    },

    /**
     * The proxy workstation (only used in NTLM authentication).
     */
    WORKSTATION(new SimpleConfigurationPropertyBuilder()
        .configKey("workstation")
        .name("Workstation")
        .defaultDescription("The proxy workstation (only used in NTLM authentication).")
        .build()) {
      @Override
      protected void setFieldValue(HttpProxyConfigProperties properties, String propertyValue) {
        properties.setWorkstation(propertyValue);
      }
    },

    /**
     * Whether the proxy should use preemptive basic authentication.
     */
    PREEMPTIVE_BASIC_PROXY_AUTH(new SimpleConfigurationPropertyBuilder()
        .configKey("preemptiveBasicProxyAuth")
        .name("Preemptive basic proxy auth")
        .required(false)
        .widget(ConfigurationProperty.Widget.CHECKBOX)
        .defaultValue("false")
        .defaultDescription("Whether the proxy should use preemptive basic authentication.")
        .defaultErrorMessage(null)
        .sensitive(false)
        .build()) {
      @Override
      protected void setFieldValue(HttpProxyConfigProperties properties, String propertyValue) {
        properties.setPreemptiveBasicProxyAuth(Boolean.parseBoolean(propertyValue));
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
    private HttpProxyConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    @Override
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }

    /**
     * Reads the property from the specified configuration, and if it is present, sets the
     * corresponding field on the HTTP proxy config properties.
     *
     * @param configuration       the configuration
     * @param properties          the HTTP proxy config properties
     * @param localizationContext the localization context
     */
    private void readAndSetPropertyValue(Configured configuration,
        HttpProxyConfigProperties properties, LocalizationContext localizationContext) {
      String propertyValue = configuration.getConfigurationValue(this, localizationContext);
      if (propertyValue != null) {
        setFieldValue(properties, propertyValue);
      }
    }

    /**
     * Sets the value of the corresponding field on the HTTP proxy config properties using the
     * specified value.
     *
     * @param properties    the HTTP proxy config properties
     * @param propertyValue the property value, which is known to be non-{@code null}
     */
    protected abstract void setFieldValue(HttpProxyConfigProperties properties,
        String propertyValue);
  }

  // Null values are okay for the following.  They denote the absence of a proxy.
  private String host = null;
  private int port = DEFAULT_PROXY_PORT;
  private String username = null;
  private String password = null;
  private String domain = null;
  private String workstation = null;
  private boolean preemptiveBasicProxyAuth = false;

  /**
   * Creates HTTP proxy configuration properties with default configuration.
   */
  public HttpProxyConfigProperties() {
  }

  /**
   * Creates HTTP proxy configuration properties with the specified configuration.
   *
   * @param configuration           the configuration
   * @param rootLocalizationContext the root localization context
   */
  public HttpProxyConfigProperties(Configured configuration,
      LocalizationContext rootLocalizationContext) {
    Preconditions.checkNotNull(configuration, "configuration is null");
    LocalizationContext localizationContext = new ChildLocalizationContext(
        rootLocalizationContext, "proxy");
    for (HttpProxyConfigurationPropertyToken property : HttpProxyConfigurationPropertyToken.values()) {
      property.readAndSetPropertyValue(configuration, this, localizationContext);
    }
  }

  public void setHost(String host) {
    LOG.info("Overriding host={} (default unset)", host);
    this.host = host;
  }

  public void setPort(int port) {
    LOG.info("Overriding port={} (default {})", host);
    this.port = port;
  }

  public void setUsername(String username) {
    LOG.info("Overriding username={} (default unset)", username);
    this.username = username;
  }

  public void setPassword(String password) {
    LOG.info("Overriding password={} (default unset)", password);
    this.password = password;
  }

  public void setDomain(String domain) {
    LOG.info("Overriding domain={} (default unset)", domain);
    this.domain = domain;
  }

  public void setWorkstation(String workstation) {
    LOG.info("Overriding workstation={} (default unset)", workstation);
    this.workstation = workstation;
  }

  public void setPreemptiveBasicProxyAuth(boolean preemptiveBasicProxyAuth) {
    LOG.info("Overriding preemptiveBasicProxyAuth={} (default {})", preemptiveBasicProxyAuth,
        this.preemptiveBasicProxyAuth);
  }

  public ProxyParameters getProxyParameters() {
    return new ProxyParameters(host, port, username, password, domain, workstation,
        preemptiveBasicProxyAuth);
  }
}
