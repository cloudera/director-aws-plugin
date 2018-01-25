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

package com.cloudera.director.aws;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.springframework.util.StringUtils.isEmpty;

import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * Credential provider to provider session credentials after assuming role n
 * times.
 */
@ThreadSafe
public class STSAssumeNRolesSessionCredentialsProvider implements AWSSessionCredentialsProvider {
  private static final int DEFAULT_DURATION_SECONDS = 900;
  private static final int DEFAULT_BLOCKING_REFRESH_DURATION_MSEC = 60 * 1000;
  private static final int DEFAULT_ASYNC_REFRESH_DURATION_MSEC = 300 * 1000;
  private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
          Thread thread = Executors.defaultThreadFactory().newThread(runnable);
          thread.setDaemon(true);
          return thread;
        }
      });

  private final AtomicReference<Credentials> credentials = new AtomicReference<>(null);
  private final int blockingRefreshDurationMsec;
  private final int asyncRefreshDurationMsec;
  private final AWSSecurityTokenServiceClientBuilder stsClientBuilder;
  private final List<RoleConfiguration> roleConfigurations;
  private final int roleSessionDurationSeconds;
  private final String scopeDownPolicy;

  private STSAssumeNRolesSessionCredentialsProvider(
      List<RoleConfiguration> roleConfigurations,
      AWSSecurityTokenServiceClientBuilder stsClientBuilder,
      int roleSessionDurationSeconds,
      String scopeDownPolicy,
      int blockingRefreshDurationMsec,
      int asyncRefreshDurationMsec) {
    checkArgument(blockingRefreshDurationMsec < asyncRefreshDurationMsec);
    checkArgument(asyncRefreshDurationMsec < roleSessionDurationSeconds * 1000);

    this.blockingRefreshDurationMsec = blockingRefreshDurationMsec;
    this.asyncRefreshDurationMsec = asyncRefreshDurationMsec;
    this.stsClientBuilder = stsClientBuilder;
    this.roleConfigurations = roleConfigurations;
    this.roleSessionDurationSeconds = roleSessionDurationSeconds;
    this.scopeDownPolicy = scopeDownPolicy;

    EXECUTOR.scheduleWithFixedDelay(
        new Runnable() {
          @Override
          public void run() {
            refreshSession(new Predicate<Credentials>() {
              @Override
              public boolean apply(@Nullable Credentials credentials) {
                return isRefreshNeeded(
                    credentials,
                    STSAssumeNRolesSessionCredentialsProvider.this.asyncRefreshDurationMsec);
              }
            });
          }
        },
        this.blockingRefreshDurationMsec,
        this.asyncRefreshDurationMsec,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public AWSSessionCredentials getCredentials() {
    refreshSession(new Predicate<Credentials>() {
      @Override
      public boolean apply(@Nullable Credentials credentials) {
        return isRefreshNeeded(credentials, blockingRefreshDurationMsec);
      }
    });

    return toAWSCredentials(this.credentials.get());
  }

  @Override
  public void refresh() {
    refreshSession(Predicates.<Credentials>alwaysTrue());
  }

  private void refreshSession(Predicate<Credentials> predicate) {
    Credentials oldCredentials = credentials.get();
    if (predicate.apply(oldCredentials)) {
      Credentials newCredentials = newSession(
          stsClientBuilder,
          roleConfigurations,
          roleSessionDurationSeconds,
          scopeDownPolicy);

      // No need to worry whether CAS succeeds or not, unless the latency
      // to call newSession() is even greater than role session duration
      credentials.compareAndSet(oldCredentials, newCredentials);
    }
  }

  private Credentials newSession(
      AWSSecurityTokenServiceClientBuilder stsClientBuilder,
      List<RoleConfiguration> roleConfigurations,
      int roleSessionDurationSeconds,
      String scopeDownPolicy) {

    AWSSecurityTokenService sts = stsClientBuilder.build();
    Credentials credentials = null;

    for (RoleConfiguration roleConfiguration : roleConfigurations) {
      AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
          .withRoleArn(roleConfiguration.getRoleArn())
          .withRoleSessionName(roleConfiguration.getRoleSessionName())
          .withExternalId(roleConfiguration.getRoleExternalId())
          .withDurationSeconds(roleSessionDurationSeconds)
          .withPolicy(scopeDownPolicy);

      AssumeRoleResult assumeRoleResult = sts.assumeRole(assumeRoleRequest);
      credentials = assumeRoleResult.getCredentials();
      sts = clone(stsClientBuilder)
          .withCredentials(new AWSStaticCredentialsProvider(toAWSCredentials(credentials)))
          .build();
    }

    checkState(credentials != null, "Retrieved credentials cannot be null");
    return credentials;
  }

  private static boolean isRefreshNeeded(Credentials credentials, int refreshDuration) {
    return credentials == null
        || credentials.getExpiration().getTime() - System.currentTimeMillis() < refreshDuration;
  }

  private static AWSSessionCredentials toAWSCredentials(Credentials credentials) {
    return new BasicSessionCredentials(
        credentials.getAccessKeyId(),
        credentials.getSecretAccessKey(),
        credentials.getSessionToken());
  }

  private static AWSSecurityTokenServiceClientBuilder clone(AWSSecurityTokenServiceClientBuilder builder) {
    AWSSecurityTokenServiceClientBuilder result = AWSSecurityTokenServiceClientBuilder
        .standard()
        .withClientConfiguration(builder.getClientConfiguration())
        .withCredentials(builder.getCredentials())
        .withEndpointConfiguration(builder.getEndpoint())
        .withMetricsCollector(builder.getMetricsCollector())
        .withRegion(builder.getRegion());

    List<RequestHandler2> requestHandlers = builder.getRequestHandlers();
    if (requestHandlers != null && !requestHandlers.isEmpty()) {
      result.setRequestHandlers(requestHandlers.toArray(new RequestHandler2[requestHandlers.size()]));
    }

    return result;
  }

  /**
   * Builder for {@linkplain STSAssumeNRolesSessionCredentialsProvider}.
   */
  public static final class Builder {
    private final List<RoleConfiguration> roleConfigurations;
    private final AWSSecurityTokenServiceClientBuilder stsClientBuilder;
    private String scopeDownPolicy;
    private int roleSessionDurationSeconds = DEFAULT_DURATION_SECONDS;
    private int blockingRefreshDurationMsec = DEFAULT_BLOCKING_REFRESH_DURATION_MSEC;
    private int asyncRefreshDurationMsec = DEFAULT_ASYNC_REFRESH_DURATION_MSEC;

    /**
     * Creates an instance of builder.
     * @param roleConfigurations a list of {@linkplain RoleConfiguration} to be assumed.
     *                           The list should be ordered from the first role to be assumed to the last
     * @param stsClientBuilder   an instance of AWS STS client builder
     */
    public Builder(
        List<RoleConfiguration> roleConfigurations,
        AWSSecurityTokenServiceClientBuilder stsClientBuilder) {
      this.stsClientBuilder = STSAssumeNRolesSessionCredentialsProvider.clone(
          requireNonNull(stsClientBuilder, "stsClient is null"));

      this.roleConfigurations = FluentIterable
          .from(requireNonNull(roleConfigurations, "roleConfigurations is null"))
          .filter(
              new Predicate<RoleConfiguration>() {
                @Override
                public boolean apply(@Nullable RoleConfiguration roleConfiguration) {
                  if (roleConfiguration != null
                      && (isEmpty(roleConfiguration.getRoleArn()) || isEmpty(roleConfiguration.getRoleSessionName()))) {
                    throw new IllegalArgumentException("You must specify both roleArn and roleSessionName");
                  }
                  return roleConfiguration != null;
                }
              })
          .toList();

      if (this.roleConfigurations.isEmpty()) {
        throw new IllegalArgumentException("You must specify at least a pair for roleArn and roleSessionName");
      }
    }

    /**
     * Gets role configurations.
     *
     * @return a list of role configuration.
     */
    public List<RoleConfiguration> getRoleConfigurations() {
      return roleConfigurations;
    }

    /**
     * Gets an instance of {@linkplain AWSSecurityTokenServiceClientBuilder}.
     * @return an instance of AWS STS service client builder.
     */
    public AWSSecurityTokenServiceClientBuilder getStsClientBuilder() {
      return stsClientBuilder;
    }

    /**
     * Gests role session duration in seconds.
     * @return role session duration in seconds
     */
    public int getRoleSessionDurationSeconds() {
      return roleSessionDurationSeconds;
    }

    /**
     * Sets role session duration in seconds.
     * @param roleSessionDurationSeconds
     */
    public Builder withRoleSessionDurationSeconds(int roleSessionDurationSeconds) {
      if (roleSessionDurationSeconds >= DEFAULT_DURATION_SECONDS && roleSessionDurationSeconds <= 3600) {
        this.roleSessionDurationSeconds = roleSessionDurationSeconds;
        return this;
      } else {
        throw new IllegalArgumentException("Assume Role session duration should be in the range of 15min - 1Hr");
      }
    }

    /**
     * Gets scope down policy of the last role.
     * @return role scope down policy
     */
    public String getScopeDownPolicy() {
      return scopeDownPolicy;
    }


    /**
     * Sets scope down plicy of the last role.
     * @param scopeDownPolicy scope down policy of the last role
     */
    public Builder withScopeDownPolicy(String scopeDownPolicy) {
      this.scopeDownPolicy = scopeDownPolicy;
      return this;
    }

    /**
     * Gets blocking refresh duration in milliseconds.
     * @return blocking refresh duration in milliseconds
     */
    public int getBlockingRefreshDurationMsec() {
      return blockingRefreshDurationMsec;
    }

    /**
     * Sets blocking refresh duration in milliseconds.
     * @param blockingRefreshDurationMsec blocking refresh duration in milliseconds
     */
    public Builder withBlockingRefreshDurationMsec(int blockingRefreshDurationMsec) {
      checkArgument(blockingRefreshDurationMsec >= 0, "negative blockingRefreshDurationMsec");
      this.blockingRefreshDurationMsec = blockingRefreshDurationMsec;
      return this;
    }

    /**
     * Gets async refresh duration in milliseconds.
     * @return async refresh duration in  milliseconds
     */
    public int getAsyncRefreshDurationMsec() {
      return asyncRefreshDurationMsec;
    }

    /**
     * Sets async refresh duration in milliseconds.
     * @param asyncRefreshDurationMsec async refresh duration in milliseconds
     */
    public Builder withAsyncRefreshDurationMsec(int asyncRefreshDurationMsec) {
      checkArgument(asyncRefreshDurationMsec >= 0, "negative asyncRefreshDurationMsec");
      this.asyncRefreshDurationMsec = asyncRefreshDurationMsec;
      return this;
    }

    /**
     * Creates a new instance of {@linkplain STSAssumeNRolesSessionCredentialsProvider}.
     * @return a new instance of STS credential provider which can assume several roles in sequence
     */
    public STSAssumeNRolesSessionCredentialsProvider build() {
      return new STSAssumeNRolesSessionCredentialsProvider(
          getRoleConfigurations(),
          getStsClientBuilder(),
          getRoleSessionDurationSeconds(),
          getScopeDownPolicy(),
          getBlockingRefreshDurationMsec(),
          getAsyncRefreshDurationMsec());
    }
  }

  /**
   * A class holding role configurations.
   */
  static public class RoleConfiguration {
    private final String roleArn;
    private final String roleSessionName;
    private final String roleExternalId;

    /**
     * Creates an instance of role configuration.
     * @param roleArn role Arn
     * @param roleSessionName role session name
     */
    public RoleConfiguration(String roleArn, String roleSessionName) {
      this(roleArn, roleSessionName, null);
    }

    /**
     * Creates an instance of role configuration.
     * @param roleArn role Arn
     * @param roleSessionName role session name
     * @param roleExternalId role external Id
     */
    public RoleConfiguration(String roleArn, String roleSessionName, String roleExternalId) {
      checkArgument(!isEmpty(roleArn), "roleArn is empty");
      checkArgument(!isEmpty(roleSessionName), "roleSessionName is empty");

      this.roleArn = roleArn;
      this.roleSessionName = roleSessionName;
      this.roleExternalId = roleExternalId;
    }

    /**
     * Gets role session name.
     * @return role session name
     */
    public String getRoleSessionName() {
      return roleSessionName;
    }

    /**
     * Gets role external Id.
     * @return role external Id
     */
    public String getRoleExternalId() {
      return roleExternalId;
    }

    /**
     * Gets role Arn.
     * @return role Arn
     */
    public String getRoleArn() {
      return roleArn;
    }

    @Override
    public String toString() {
      return "RoleConfiguration{" +
          "roleArn='" + roleArn + '\'' +
          ", roleSessionName='" + roleSessionName + '\'' +
          ", roleExternalId='" + roleExternalId + '\'' +
          '}';
    }
  }
}
