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

import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.ACCESS_KEY_ID;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.SECRET_ACCESS_KEY;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.SESSION_TOKEN;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v1.provider.CredentialsProvider;
import com.cloudera.director.spi.v1.provider.CredentialsProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.SimpleCredentialsProviderMetadata;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.google.common.base.Optional;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A credentials provider for AWS.
 */
public class AWSCredentialsProviderChainProvider
    implements CredentialsProvider<AWSCredentialsProviderChain> {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSCredentialsProviderChainProvider.class);

  /**
   * The credentials provider metadata.
   */
  public static final CredentialsProviderMetadata METADATA =
      new SimpleCredentialsProviderMetadata(
          AWSConfigCredentialsProvider.getConfigurationProperties());

  @Override
  public CredentialsProviderMetadata getMetadata() {
    return METADATA;
  }

  @Override
  public AWSCredentialsProviderChain createCredentials(Configured config,
      LocalizationContext localizationContext) {
    return new AWSCredentialsProviderChain(
        new AWSConfigCredentialsProvider(config, localizationContext),
        new DefaultAWSCredentialsProviderChain()
    );
  }

  /**
   * Config-based AWS credentials provider.
   */
  // Fully qualifying class names due to compiler bug
  public static class AWSConfigCredentialsProvider
      extends com.cloudera.director.spi.v1.model.util.AbstractConfigured
      implements com.amazonaws.auth.AWSCredentialsProvider {

    /**
     * The list of configuration properties (including inherited properties).
     */
    private static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
        ConfigurationPropertiesUtil.asConfigurationPropertyList(
            AWSConfigCredentialsProviderConfigurationPropertyToken.values());

    /**
     * Returns the list of configuration properties for creating credentials.
     *
     * @return the list of configuration properties for creating credentials
     */
    public static List<ConfigurationProperty> getConfigurationProperties() {
      return CONFIGURATION_PROPERTIES;
    }

    /**
     * AWS credentials configuration property tokens.
     */
    // Fully qualifying class name due to compiler bug
    public static enum AWSConfigCredentialsProviderConfigurationPropertyToken
        implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

      /**
       * An alphanumeric text string that uniquely identifies the user who
       * owns the account. No two accounts can have the same AWS Access Key.
       *
       * @see <a href="http://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html">AWS Security Credentials</a>
       */
      ACCESS_KEY_ID(new SimpleConfigurationPropertyBuilder()
          .configKey("accessKeyId")
          .name("Access key ID")
          .defaultDescription("The AWS access key ID.<br />"
              + "Leave unset to get Amazon Web Services credentials from the OS environment.")
          .defaultErrorMessage("AWS credentials configuration is missing an access key ID")
          .sensitive(true)
          .build()),

      /**
       * This key plays the role of a  password. Together with the access key
       * this forms a secure pair that confirms the user's identity
       *
       * @see <a href="http://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html">AWS Security Credentials</a>
       */
      SECRET_ACCESS_KEY(new SimpleConfigurationPropertyBuilder()
          .configKey("secretAccessKey")
          .name("Secret access key")
          .widget(ConfigurationProperty.Widget.PASSWORD)
          .defaultDescription("The AWS secret access key.")
          .defaultErrorMessage("AWS credentials configuration is missing a secret access key")
          .sensitive(true)
          .build()),

      /**
       * The token that users must pass to the service API to use the temporary credentials.
       *
       * @see <a href="http://docs.aws.amazon.com/STS/latest/APIReference/API_Credentials.html">API Credentials</a>
       * @see <a href="http://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html">API AssumeRole</a>
       */
      SESSION_TOKEN(new SimpleConfigurationPropertyBuilder()
          .configKey("sessionToken")
          .name("Session token")
          .defaultDescription("The AWS session token.")
          .sensitive(true)
          .hidden(true)
          .build());

      /**
       * The configuration property.
       */
      private final ConfigurationProperty configurationProperty;

      /**
       * Creates a configuration property token with the specified parameters.
       *
       * @param configurationProperty the configuration property
       */
      private AWSConfigCredentialsProviderConfigurationPropertyToken(
          ConfigurationProperty configurationProperty) {
        this.configurationProperty = configurationProperty;
      }

      @Override
      public ConfigurationProperty unwrap() {
        return configurationProperty;
      }
    }

    /**
     * The AWS access key.
     */
    private final Optional<String> accessKeyId;

    /**
     * The AWS secret access key.
     */
    private final Optional<String> secretAccessKey;

    /**
     * The optional session token.
     */
    private final Optional<String> sessionToken;

    /**
     * Creates a config-based AWS credentials provider with the specified parameters.
     *
     * @param configuration       the configuration
     * @param localizationContext the localization context
     */
    public AWSConfigCredentialsProvider(Configured configuration,
        LocalizationContext localizationContext) {
      super(configuration);
      accessKeyId = getOptionalConfigurationValue(ACCESS_KEY_ID, localizationContext);
      secretAccessKey = getOptionalConfigurationValue(SECRET_ACCESS_KEY, localizationContext);
      sessionToken = getOptionalConfigurationValue(SESSION_TOKEN, localizationContext);
      if (!(accessKeyId.isPresent() && secretAccessKey.isPresent())) {
        LOG.info("AWS credentials not fully specified."
            + " Credentials provider chain fallback will be used.");
      }
    }

    @Override
    public AWSCredentials getCredentials() {
      if (!(accessKeyId.isPresent() && secretAccessKey.isPresent())) {
        return null;
      }
      String accessKeyIdValue = accessKeyId.get();
      String secretAccessKeyValue = secretAccessKey.get();
      if (sessionToken.isPresent()) {
        return new BasicSessionCredentials(accessKeyIdValue, secretAccessKeyValue,
            sessionToken.get());
      } else {
        return new BasicAWSCredentials(accessKeyIdValue, secretAccessKeyValue);
      }
    }

    @Override
    public void refresh() {
      // no-op, credentials are immutable
    }

    /**
     * Returns the value of the specified configuration property, or the default value (with
     * {@code null} replaced by an absent {@code Optional}) if the value is not present and the
     * configuration property is optional.
     *
     * @param token               the configuration property token
     * @param localizationContext the localization context
     * @return the value of the specified configuration property, or the default value (with
     * {@code null} replaced by an absent {@code Optional}) if the value is not present and the
     * configuration property is optional
     * @throws IllegalArgumentException if the specified configuration property is not present and
     *                                  required
     */
    private Optional<String> getOptionalConfigurationValue(
        com.cloudera.director.spi.v1.model.ConfigurationPropertyToken token,
        LocalizationContext localizationContext) {
      return Optional.fromNullable(getConfigurationValue(token.unwrap(), localizationContext));
    }
  }
}
