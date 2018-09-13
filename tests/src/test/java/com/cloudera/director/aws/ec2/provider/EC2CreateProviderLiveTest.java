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

package com.cloudera.director.aws.ec2.provider;

import com.cloudera.director.aws.AWSCredentialsProviderChainProvider;
import com.cloudera.director.aws.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyValue;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.aws.test.EC2ProviderCommon.getEc2Provider;
import static com.cloudera.director.aws.test.EC2ProviderCommon.isLive;
import static com.cloudera.director.aws.test.EC2ProviderCommon.putConfig;
import static com.cloudera.director.spi.v2.provider.Launcher.DEFAULT_PLUGIN_LOCALIZATION_CONTEXT;
import static org.junit.Assume.assumeTrue;

/**
 * This test makes sure we can successfully instantiate the EC2Provider for
 * all valid regions. This ensures that the region endpoints can be set for
 * all the AWS clients used in the EC2Provider.
 */
@RunWith(Parameterized.class)
public class EC2CreateProviderLiveTest {

  private void createEC2Provider(String region) {
    Map<String, String> credentialsConfigMap = new LinkedHashMap<>();
    SimpleConfiguration credentialsConfig = new SimpleConfiguration(credentialsConfigMap);
    AWSCredentialsProvider credentialsProvider =
        new AWSCredentialsProviderChainProvider()
            .createCredentials(credentialsConfig, DEFAULT_PLUGIN_LOCALIZATION_CONTEXT);

    // Configure provider
    LinkedHashMap<String, String> providerConfigMap = new LinkedHashMap<>();
    putConfig(providerConfigMap, REGION, region);

    // Create provider
    getEc2Provider(new SimpleConfiguration(providerConfigMap), credentialsProvider);
  }

  @Parameterized.Parameters
  public static Iterable<? extends Object> data() {
    List<String> regionsToTest = new ArrayList<>();

    for (ConfigurationPropertyValue config : REGION.unwrap().getValidValues(DEFAULT_PLUGIN_LOCALIZATION_CONTEXT)) {
      String region = config.getValue();
      regionsToTest.add(region);
    }

    return regionsToTest;
  }

  private String region;

  public EC2CreateProviderLiveTest(String region) {
    this.region = region;
  }

  @Test
  public void testCreateProvider() {
    assumeTrue(isLive());
    createEC2Provider(region);
  }
}
