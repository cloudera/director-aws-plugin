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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import com.cloudera.director.aws.provider.AWSProvider;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.provider.CloudProvider;
import com.cloudera.director.spi.v2.provider.CloudProviderMetadata;
import com.cloudera.director.spi.v2.provider.Launcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests {@link AWSLauncher}.
 */
public class AWSLauncherTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testLauncher() throws InterruptedException, IOException {

    Launcher launcher = new AWSLauncher();
    launcher.initialize(temporaryFolder.getRoot(), null);

    assertEquals(1, launcher.getCloudProviderMetadata().size());
    CloudProviderMetadata metadata = launcher.getCloudProviderMetadata().get(0);

    assertEquals(AWSProvider.ID, metadata.getId());

    List<ConfigurationProperty> providerConfigurationProperties =
        metadata.getProviderConfigurationProperties();
    assertEquals(0, providerConfigurationProperties.size());

    List<ConfigurationProperty> credentialsConfigurationProperties =
        metadata.getCredentialsProviderMetadata().getCredentialsConfigurationProperties();
    assertEquals(5, credentialsConfigurationProperties.size());
    assertTrue(credentialsConfigurationProperties.contains(ACCESS_KEY_ID.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(SECRET_ACCESS_KEY.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(SESSION_TOKEN.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(DELEGATED_ROLE_ARN.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(DELEGATED_ROLE_EXTERNAL_ID.unwrap()));

    CloudProvider cloudProvider = launcher.createCloudProvider(
        AWSProvider.ID,
        new SimpleConfiguration(Collections.<String, String>emptyMap()),
        Locale.getDefault());

    assertEquals(AWSProvider.class, cloudProvider.getClass());

    CloudProvider cloudProvider2 = launcher.createCloudProvider(
        AWSProvider.ID, new SimpleConfiguration(Collections.<String, String>emptyMap()), Locale.getDefault());
    assertNotSame(cloudProvider, cloudProvider2);
  }

  @Test
  public void testLauncherConfigEmpty() {
    AWSLauncher launcher = new AWSLauncher();
    launcher.initialize(temporaryFolder.getRoot(), null);

    assertNotNull(launcher.awsTimeouts);
    assertTrue(launcher.stsRoles.getRoleConfigurations().isEmpty());
  }

  @Test
  public void testLauncherConfig() throws InterruptedException, IOException {
    AWSLauncher launcher = new AWSLauncher();
    File configDir = temporaryFolder.getRoot();
    File configFile = new File(configDir, Configurations.CONFIGURATION_FILE_NAME);
    PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(configFile), "UTF-8")));
    printWriter.println("httpProxy {");
    printWriter.println("}");
    printWriter.println("ephemeralDeviceMappings {");
    printWriter.println("}");
    printWriter.println("virtualizationMappings {");
    printWriter.println("}");
    printWriter.println("awsClient {");
    printWriter.println("  maxErrorRetries: 8");
    printWriter.println("}");
    printWriter.println("awsTimeouts {");
    printWriter.println("  ec2 {");
    printWriter.println("    ebs {");
    printWriter.println("      availableSeconds: 123");
    printWriter.println("    }");
    printWriter.println("  }");
    printWriter.println("}");
    printWriter.println(Configurations.STS_ROLES_SECTION + ": [");
    printWriter.println("  {");
    printWriter.println("    " + STSRoles.ROLE_ARN + ": \"arn:aws:iam::123456789012:role/roleSwitch\"");
    printWriter.println("    " + STSRoles.ROLE_SESSION_NAME + ": reader");
    printWriter.println("    " + STSRoles.ROLE_EXTERNAL_ID + ": token");
    printWriter.println("  }");
    printWriter.println("]");
    printWriter.close();
    launcher.initialize(configDir, null);
    assertEquals(8, launcher.awsClientConfig.getMaxErrorRetries());
    assertEquals(123L, launcher.awsTimeouts.getTimeout("ec2.ebs.availableSeconds").get().longValue());
    assertEquals(1, launcher.stsRoles.getRoleConfigurations().size());
  }
}
