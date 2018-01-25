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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSSessionCredentials;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.securitytoken.model.Credentials;
import com.cloudera.director.aws.shaded.com.google.common.collect.ImmutableList;

import java.util.Date;
import java.util.concurrent.atomic.AtomicStampedReference;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class STSAssumeNRolesSessionCredentialsProviderTest {
  @Test
  public void testGetCredentialsFromSingleThreadIsIncreasingExpirationTime() throws InterruptedException {
    final String accessKey = "accessKey";
    final AtomicStampedReference<Long> currentRefresher = new AtomicStampedReference<>(-1L, 0);

    AWSSecurityTokenServiceClientBuilder stsClientBuilder = spy(AWSSecurityTokenServiceClientBuilder.class);
    AWSSecurityTokenService stsClient = mock(AWSSecurityTokenService.class);
    stsClientBuilder.withRegion("us-west-1");
    doReturn(stsClient).when(stsClientBuilder).build();
    doReturn(stsClientBuilder).when(stsClientBuilder).withCredentials(any(AWSCredentialsProvider.class));
    when(stsClient.assumeRole(any(AssumeRoleRequest.class))).thenAnswer(new Answer<AssumeRoleResult>() {
      @Override
      public AssumeRoleResult answer(InvocationOnMock invocationOnMock) throws Throwable {
        long threadId = Thread.currentThread().getId();
        int[] stamps = new int[1];
        int newStamp;
        while (true) {
          Long oldThreadId = currentRefresher.get(stamps);
          newStamp = stamps[0] + 1;
          if (currentRefresher.compareAndSet(oldThreadId, threadId, stamps[0], newStamp)) {
            break;
          }
        }
        Credentials credentials = new Credentials(
            accessKey,
            String.valueOf(threadId),
            String.valueOf(newStamp),
            new Date(newStamp));
        return new AssumeRoleResult().withCredentials(credentials);
      }
    });

    STSAssumeNRolesSessionCredentialsProvider.RoleConfiguration rc =
        new STSAssumeNRolesSessionCredentialsProvider.RoleConfiguration("roleArn", "roleSessionName");
    STSAssumeNRolesSessionCredentialsProvider.Builder builder =
        new STSAssumeNRolesSessionCredentialsProvider.Builder(ImmutableList.of(rc), stsClientBuilder)
            .withBlockingRefreshDurationMsec(0)
            .withAsyncRefreshDurationMsec(1)
            .withRoleSessionDurationSeconds(900);
    STSAssumeNRolesSessionCredentialsProvider.Builder spyBuilder = spy(builder);
    when(spyBuilder.getStsClientBuilder()).thenReturn(stsClientBuilder);
    STSAssumeNRolesSessionCredentialsProvider stsAssumeNRolesSessionCredentialsProvider = spyBuilder.build();

    AWSSessionCredentials credentials1 = stsAssumeNRolesSessionCredentialsProvider.getCredentials();
    for (int i = 0; i < 100; ++i) {
      Thread.sleep(1);
      AWSSessionCredentials credentials2 = stsAssumeNRolesSessionCredentialsProvider.getCredentials();
      assertThat(Integer.parseInt(credentials2.getSessionToken()))
          .isGreaterThan(Integer.parseInt(credentials1.getSessionToken()));
      credentials1 = credentials2;
    }
  }
}

