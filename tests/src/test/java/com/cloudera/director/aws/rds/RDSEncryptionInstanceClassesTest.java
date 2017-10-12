// (c) Copyright 2016 Cloudera, Inc.
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

package com.cloudera.director.aws.rds;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.cloudera.director.aws.rds.RDSEncryptionInstanceClasses.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Locale;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RDSEncryptionInstanceClassesTest {

  private final Map<String, String> configurationMap = ImmutableMap.<String, String>builder()
      .put(ConfigurationPropertyToken.CUSTOM_ENCRYPTION_INSTANCE_CLASSES_PATH.unwrap().getConfigKey(),
           "customencryptioninstanceclasses.properties")
      .build();

  private static File configurationDirectory;

  @BeforeClass
  public static void setUpClass() {
    try {
      configurationDirectory = new File(Resources.getResource("com/cloudera/director/aws/rds").toURI());
    } catch (Exception e) {
      fail("Unable to locate configuration directory");
    }
  }

  private static final List<String> TEST_INSTANCE_CLASSES =
      ImmutableList.of("class1", "class2", "class4");
  private static final LocalizationContext LOCALIZATION_CONTEXT =
      DefaultLocalizationContext.FACTORY.createRootLocalizationContext(Locale.US);

  private SimpleConfiguration configuration;
  private RDSEncryptionInstanceClasses testInstanceClasses;

  @Before
  public void setUp() {
    testInstanceClasses =
        RDSEncryptionInstanceClasses.getTestInstance(TEST_INSTANCE_CLASSES, LOCALIZATION_CONTEXT);
  }

  @Test
  public void testApply() {
    assertThat(testInstanceClasses.apply("class1")).isTrue();
    assertThat(testInstanceClasses.apply("class3")).isFalse();
  }

  // this test must remained synchronized with the internal instance class list
  @Test
  public void testDefaultList() throws Exception {
    configuration = new SimpleConfiguration(ImmutableMap.<String, String>of());
    testInstanceClasses = new RDSEncryptionInstanceClasses(configuration, configurationDirectory,
                                                           LOCALIZATION_CONTEXT);

    assertThat(testInstanceClasses.apply("db.m4.large")).isTrue();
    assertThat(testInstanceClasses.apply("db.m3.2xlarge")).isTrue();
    assertThat(testInstanceClasses.apply("nope")).isFalse();
  }

  // this test must remained synchronized with the internal instance class list
  @Test
  public void testCustomList() throws Exception {
    configuration = new SimpleConfiguration(configurationMap);
    testInstanceClasses = new RDSEncryptionInstanceClasses(configuration, configurationDirectory,
                                                           LOCALIZATION_CONTEXT);

    assertThat(testInstanceClasses.apply("db.m4.large")).isFalse();
    assertThat(testInstanceClasses.apply("db.m3.2xlarge")).isTrue();
    assertThat(testInstanceClasses.apply("db.x1.large")).isTrue();
    assertThat(testInstanceClasses.apply("nope")).isFalse();
  }

}
