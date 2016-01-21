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

package com.cloudera.director.aws.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.cloudera.director.aws.shaded.org.springframework.core.env.PropertyResolver;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

public class PropertyResolversTest {

  private static final String BUILT_IN_PATH = "classpath:builtin.properties";
  private static final String CUSTOM1_PATH = "classpath:custom1.properties";
  private static final String CUSTOM2_PATH = "classpath:custom2.properties";
  private static final String CUSTOMX_PATH = "classpath:customx.properties";

  @Test
  public void testEmptyPropertyResolver() {
    PropertyResolver pr = PropertyResolvers.newEmptyPropertyResolver();
    assertThat(pr.getProperty("property")).isNull();
  }

  @Test
  public void testMapPropertyResolver() throws Exception {
    Map<String, String> map = Maps.newHashMap();
    map.put("property1", "value1");
    map.put("property2", "value2");
    PropertyResolver pr = PropertyResolvers.newMapPropertyResolver(map);

    assertThat(pr.getProperty("property1")).isEqualTo("value1");
    assertThat(pr.getProperty("property2")).isEqualTo("value2");
  }

  @Test
  public void testMultiResourcePropertyResolver() throws Exception {
    PropertyResolver pr = PropertyResolvers.newMultiResourcePropertyResolver(
        BUILT_IN_PATH,
        CUSTOM1_PATH,
        CUSTOM2_PATH);

    assertThat(pr.getProperty("property1")).isEqualTo("value1b");
    assertThat(pr.getProperty("property2")).isEqualTo("value2");
    assertThat(pr.getProperty("property3")).isEqualTo("value3a");
    assertThat(pr.getProperty("property4")).isEqualTo("value4b");
    assertThat(pr.getProperty("property5")).isEqualTo("value5b");
  }

  @Test
  public void testMultiResourcePropertyResolverWithMissing() throws Exception {
    PropertyResolver pr = PropertyResolvers.newMultiResourcePropertyResolver(
        BUILT_IN_PATH,
        CUSTOMX_PATH,
        CUSTOM2_PATH);

    assertThat(pr.getProperty("property1")).isEqualTo("value1b");
    assertThat(pr.getProperty("property2")).isEqualTo("value2");
    assertThat(pr.getProperty("property3")).isEqualTo("value3");
    assertThat(pr.getProperty("property4")).isEqualTo("value4b");
    assertThat(pr.getProperty("property5")).isEqualTo("value5b");
  }

  @Test(expected = IOException.class)
  public void testMultiResourcePropertyResolverFailOnMissing() throws Exception {
    PropertyResolver pr = PropertyResolvers.newMultiResourcePropertyResolver(
        false,
        BUILT_IN_PATH,
        CUSTOMX_PATH,
        CUSTOM2_PATH);
  }

  @Test(expected = IOException.class)
  public void testMultiResourcePropertyResolverFailOnMissingBuiltIn() throws Exception {
    PropertyResolver pr = PropertyResolvers.newMultiResourcePropertyResolver(
        CUSTOMX_PATH,
        CUSTOM2_PATH);
  }

  @Test(expected = NullPointerException.class)
  public void testMultiResourcePropertyResolverFailOnNullBuiltIn() throws Exception {
    PropertyResolver pr = PropertyResolvers.newMultiResourcePropertyResolver(
        null,
        CUSTOM1_PATH,
        CUSTOM2_PATH);
  }

  @Test(expected = NullPointerException.class)
  public void testMultiResourcePropertyResolverFailOnNullCustom() throws Exception {
    PropertyResolver pr = PropertyResolvers.newMultiResourcePropertyResolver(
        BUILT_IN_PATH,
        null,
        CUSTOM2_PATH);
  }

}
