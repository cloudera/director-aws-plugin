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

import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProviderProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.ACCESS_KEY_ID;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProviderProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.DELEGATED_ROLE_ARN;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProviderProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.DELEGATED_ROLE_EXTERNAL_ID;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProviderProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.SECRET_ACCESS_KEY;
import static com.cloudera.director.aws.AWSCredentialsProviderChainProvider.AWSConfigCredentialsProviderProvider.AWSConfigCredentialsProviderConfigurationPropertyToken.SESSION_TOKEN;
import static com.cloudera.director.aws.STSRoles.getTimestampedSessionName;
import static com.cloudera.director.aws.STSRoles.parseRoleName;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.cloudera.director.aws.STSAssumeNRolesSessionCredentialsProvider.RoleConfiguration;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v2.provider.CredentialsProvider;
import com.cloudera.director.spi.v2.provider.CredentialsProviderMetadata;
import com.cloudera.director.spi.v2.provider.util.SimpleCredentialsProviderMetadata;
import com.cloudera.director.spi.v2.util.ConfigurationPropertiesUtil;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A credentials provider for AWS.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class AWSCredentialsProviderChainProvider
    implements CredentialsProvider<AWSCredentialsProvider> {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSCredentialsProviderChainProvider.class);

  private final List<RoleConfiguration> roleConfigurations;

  /**
   * Creates an instance of AWS crendential provider chain provider.
   */
  public AWSCredentialsProviderChainProvider() {
    this(null);
  }

  /**
   * Creates an instance of AWS crendential provider chain provider.
   * @param roleConfigurations the role configurations
   */
  public AWSCredentialsProviderChainProvider(List<RoleConfiguration> roleConfigurations) {
    this.roleConfigurations = roleConfigurations != null
        ? ImmutableList.copyOf(roleConfigurations)
        : Collections.<RoleConfiguration>emptyList();
  }

  /**
   * The credentials provider metadata.
   */
  public static final CredentialsProviderMetadata METADATA =
      new SimpleCredentialsProviderMetadata(
          AWSConfigCredentialsProviderProvider.getConfigurationProperties());

  @Override
  public CredentialsProviderMetadata getMetadata() {
    return METADATA;
  }

  @Override
  public AWSCredentialsProvider createCredentials(
      Configured config,
      LocalizationContext localizationContext) {
    return new AWSConfigCredentialsProviderProvider(roleConfigurations, config, localizationContext)
        .getCredentialsProvider();
  }

  /**
   * Config-based AWS credentials provider provider.
   */
  // Fully qualifying class names due to compiler bug
  public static class AWSConfigCredentialsProviderProvider
      extends com.cloudera.director.spi.v2.model.util.AbstractConfigured {

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
    public enum AWSConfigCredentialsProviderConfigurationPropertyToken
        implements com.cloudera.director.spi.v2.model.ConfigurationPropertyToken {

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
      DELEGATED_ROLE_ARN(new SimpleConfigurationPropertyBuilder()
          .configKey("delegatedRoleArn")
          .name("Delegated Role ARN")
          .defaultDescription("The Amazon Resource Name (ARN) of the role to assume.")
          .sensitive(false)
          .hidden(true)
          .build()),

      /**
       * A unique identifier that is used by third parties when assuming roles
       * in their customers' accounts. It is used to prevent the confused
       * deputy problem.
       *
       * @see <a href="http://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html">
       * How to Use an External ID When Granting Access to Your AWS Resources to a Third Party</a>
       */
      DELEGATED_ROLE_EXTERNAL_ID(new SimpleConfigurationPropertyBuilder()
          .configKey("delegatedRoleExternalId")
          .name("Delegated Role External ID")
          .defaultDescription("The external id for delegated role access.")
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
      AWSConfigCredentialsProviderConfigurationPropertyToken(
          ConfigurationProperty configurationProperty) {
        this.configurationProperty = configurationProperty;
      }

      @Override
      public ConfigurationProperty unwrap() {
        return configurationProperty;
      }
    }

    /**
     * The AWS credentials provider.
     */
    private final AWSCredentialsProvider credentialsProvider;

    /**
     * The duration, in seconds, of the role session. The value can range from
     * 900 seconds (15 minutes) to 3600 seconds (1 hour). By default, the value
     * is set to 3600 seconds.
     */
    private final static int ASSUME_ROLE_DURATION_SECONDS = 3600;

    private final static DefaultAwsRegionProviderChain DEFAULT_REGION_PROVIDER =
        new DefaultAwsRegionProviderChain();

    private final static String STS_GLOBAL_ENDPOINT = "sts.amazonaws.com";

    /**
     * Creates a config-based AWS credentials provider with the specified parameters.
     *
     * @param roleConfigurations  the role configurations
     * @param configuration       the configuration
     * @param localizationContext the localization context
     */
    @SuppressWarnings("PMD.UselessParentheses")
    public AWSConfigCredentialsProviderProvider(
        List<RoleConfiguration> roleConfigurations,
        Configured configuration,
        LocalizationContext localizationContext) {
      super(configuration);
      Optional<String> optionalAccessKeyId =
          getOptionalConfigurationValue(ACCESS_KEY_ID, localizationContext);
      Optional<String> optionalSecretAccessKey =
          getOptionalConfigurationValue(SECRET_ACCESS_KEY, localizationContext);
      if (optionalAccessKeyId.isPresent() && optionalSecretAccessKey.isPresent()) {
        String accessKeyId = optionalAccessKeyId.get();
        String secretAccessKey = optionalSecretAccessKey.get();
        Optional<String> sessionToken =
            getOptionalConfigurationValue(SESSION_TOKEN, localizationContext);
        credentialsProvider = new AWSStaticCredentialsProvider(
            sessionToken.isPresent()
                ? new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken.get())
                : new BasicAWSCredentials(accessKeyId, secretAccessKey)
        );
      } else {
        Optional<String> roleArn =
            getOptionalConfigurationValue(DELEGATED_ROLE_ARN, localizationContext);
        Optional<String> externalId =
            getOptionalConfigurationValue(DELEGATED_ROLE_EXTERNAL_ID, localizationContext);
        if (roleArn.isPresent() && externalId.isPresent()) {
          roleConfigurations = FluentIterable
              .from(roleConfigurations)
              .append(new RoleConfiguration(
                  roleArn.get(),
                  getTimestampedSessionName(parseRoleName(roleArn.get())),
                  externalId.get()))
              .toList();
        }

        if (!roleConfigurations.isEmpty()) {
          credentialsProvider = createAssumeRoleSessionCredentialsProvider(roleConfigurations);
        } else {
          LOG.info("AWS credentials not fully specified."
              + " Credentials provider chain fallback will be used.");
          credentialsProvider = new DefaultAWSCredentialsProviderChain();
        }
      }
    }

    /**
     * Returns the AWS credentials provider.
     *
     * @return the AWS credentials provider
     */
    public AWSCredentialsProvider getCredentialsProvider() {
      return credentialsProvider;
    }

    /**
     * Creates the STS assume role session credentials provider.
     *
     * @param roleConfigurations the STS role configurations
     * @return an AWS session credentials provider
     */
    private AWSSessionCredentialsProvider createAssumeRoleSessionCredentialsProvider(
        List<RoleConfiguration> roleConfigurations) {
      LOG.info(
          "Building provider to assume role {} with duration {} in seconds",
          roleConfigurations,
          ASSUME_ROLE_DURATION_SECONDS);

      return new STSAssumeNRolesSessionCredentialsProvider
          .Builder(roleConfigurations, getSTSClientBuilder())
          .withRoleSessionDurationSeconds(ASSUME_ROLE_DURATION_SECONDS)
          .build();
    }

    private AWSSecurityTokenServiceClientBuilder getSTSClientBuilder() {
      AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard();

      try {
        builder.withRegion(DEFAULT_REGION_PROVIDER.getRegion());
      } catch (AmazonClientException e) {
        LOG.warn("failed to retrieve region, falling back to STS global endpoint", e);
        builder.withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(STS_GLOBAL_ENDPOINT, null));
      }

      return builder;
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
        com.cloudera.director.spi.v2.model.ConfigurationPropertyToken token,
        LocalizationContext localizationContext) {
      return Optional.fromNullable(getConfigurationValue(token.unwrap(), localizationContext));
    }
  }
}
