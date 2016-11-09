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
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.ROLE_ARN;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.SECRET_ACCESS_KEY;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.SESSION_TOKEN;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v1.provider.CredentialsProvider;
import com.cloudera.director.spi.v1.provider.CredentialsProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.SimpleCredentialsProviderMetadata;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.google.common.base.Optional;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A credentials provider for AWS.
 */
public class AWSCredentialsProviderChainProvider
    implements CredentialsProvider<AWSCredentialsProviderChain> {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSCredentialsProviderChainProvider.class);

  private final AWSSecurityTokenServiceClient sts;

  public AWSCredentialsProviderChainProvider(AWSSecurityTokenServiceClient sts) {
    this.sts = sts;
  }

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
        new AWSConfigCredentialsProvider(config, localizationContext,
                                         this.sts),
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
          .build()),

      /**
       * The Amazon Resource Name (ARN) of the role to assume.
       *
       * @see <a href="http://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html">API AssumeRole</a>
       */
      ROLE_ARN(new SimpleConfigurationPropertyBuilder()
          .configKey("roleArn")
          .name("Role ARN")
          .defaultDescription("The Amazon Resource Name (ARN) of the role to assume.")
          .sensitive(false)
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
     * The optional Amazon Resource Name (ARN) of the role to assume.
     */
    private final Optional<String> roleArn;

    /**
     * Client for accessing AWS STS (Security Token Service).
     * All service calls made using this client are blocking, and will not
     * return until the service call completes.
     */
    private final AWSSecurityTokenServiceClient stsClient;

    /**
     * The temporary credentials that were used last time.
     */
    private Credentials lastTempCredentials;

    /**
     * The duration, in seconds, of the role session. The value can range from
     * 900 seconds (15 minutes) to 3600 seconds (1 hour). By default, the value
     * is set to 3600 seconds.
     */
    private final static int ASSUME_ROLE_DURATION_SECONDS = 3600;

    /**
     * Creates a config-based AWS credentials provider with the specified parameters.
     *
     * @param configuration       the configuration
     * @param localizationContext the localization context
     */
    @SuppressWarnings("PMD.UselessParentheses")
    public AWSConfigCredentialsProvider(Configured configuration,
                                        LocalizationContext localizationContext,
                                        AWSSecurityTokenServiceClient sts) {
      super(configuration);
      accessKeyId = getOptionalConfigurationValue(ACCESS_KEY_ID, localizationContext);
      secretAccessKey = getOptionalConfigurationValue(SECRET_ACCESS_KEY, localizationContext);
      sessionToken = getOptionalConfigurationValue(SESSION_TOKEN, localizationContext);
      roleArn = getOptionalConfigurationValue(ROLE_ARN, localizationContext);
      stsClient = sts;
      if (!((accessKeyId.isPresent() && secretAccessKey.isPresent()) ||
          (roleArn.isPresent() && stsClient != null))) {
        LOG.info("AWS credentials not fully specified."
            + " Credentials provider chain fallback will be used.");
      }
    }

    /**
     * Gets temporary security credentials (consisting of an access key ID,
     * a secret access key, and a security token) that director can use to
     * access AWS resources across the account defined by the role ARN.
     *
     * If the last used temporary credentials will not expire within the next
     * 10 seconds, we will simply re-use the same credentials.
     *
     * @return temporary AWS security credentials
     */
    private BasicSessionCredentials assumeRole() {
      // Ten seconds as a buffer to make sure the temporary credentials are
      // still valid when they are used.
      final long bufferInMillis = 10000L;
      if (lastTempCredentials != null &&
          lastTempCredentials.getExpiration().after(
              new Date(System.currentTimeMillis() + bufferInMillis))) {
        return new BasicSessionCredentials(
            lastTempCredentials.getAccessKeyId(),
            lastTempCredentials.getSecretAccessKey(),
            lastTempCredentials.getSessionToken()
        );
      }

      /* The role session name uniquely identifies a session when the same
       * role is assumed by different principals or for different reasons.
       * In cross-account scenarios, the role session name is visible to, and
       * can be logged by the account that owns the role. The role session name
       * is also used in the ARN of the assumed role principal. This means that
       * subsequent cross-account API requests using the temporary security
       * credentials will expose the role session name to the external account
       * in their CloudTrail logs.
       *
       * The format for the session name, as described by its regex pattern, is
       * a string of characters consisting of upper- and lower-case alphanumeric
       * characters with no spaces. You can also include any of the following
       * characters: =,.@-
       */
      final String roleSessionName = UUID.randomUUID().toString();
      AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
          .withRoleArn(roleArn.get())
          .withRoleSessionName(roleSessionName)
          .withDurationSeconds(ASSUME_ROLE_DURATION_SECONDS);
      LOG.info("Assume role: {} with session name: {} and duration in {} seconds",
               roleArn.get(), roleSessionName, ASSUME_ROLE_DURATION_SECONDS);

      AssumeRoleResult assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);
      lastTempCredentials = assumeRoleResult.getCredentials();
      return new BasicSessionCredentials(
          lastTempCredentials.getAccessKeyId(),
          lastTempCredentials.getSecretAccessKey(),
          lastTempCredentials.getSessionToken()
      );
    }

    @Override
    public AWSCredentials getCredentials() {
      if (accessKeyId.isPresent() && secretAccessKey.isPresent()) {
        String accessKeyIdValue = accessKeyId.get();
        String secretAccessKeyValue = secretAccessKey.get();
        if (sessionToken.isPresent()) {
          return new BasicSessionCredentials(accessKeyIdValue, secretAccessKeyValue,
                                             sessionToken.get());
        } else {
          return new BasicAWSCredentials(accessKeyIdValue, secretAccessKeyValue);
        }
      }
      if (roleArn.isPresent() && stsClient != null) {
        return assumeRole();
      }
      return null;
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
