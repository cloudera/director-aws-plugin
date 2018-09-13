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

import static com.cloudera.director.aws.common.HoconConfigUtils.asConfigKey;
import static com.cloudera.director.aws.common.HoconConfigUtils.asConfigPath;
import static com.cloudera.director.aws.common.HoconConfigUtils.getStringMap;
import static com.cloudera.director.aws.common.HoconConfigUtils.stripQuotes;
import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigException;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Tests {@code ConfigUtils}.
 */
public class HoconConfigUtilsTest {

  @Test
  public void testAsConfigKey() {
    assertThat(asConfigKey(null)).isNull();
    assertThat(asConfigKey("")).isEqualTo("");
    assertThat(asConfigKey("\"")).isEqualTo("\"");
    assertThat(asConfigKey("a")).isEqualTo("a");
    assertThat(asConfigKey("a.b")).isEqualTo("\"a.b\"");
    assertThat(asConfigKey("\"a.b\"")).isEqualTo("\"a.b\"");
  }

  @Test
  public void testAsConfigPath() {
    assertThat(asConfigPath(null)).isNull();
    assertThat(asConfigPath("")).isEqualTo("");
    assertThat(asConfigPath("a")).isEqualTo("a");
    assertThat(asConfigPath("\"")).isEqualTo("\"");
    assertThat(asConfigPath("\"a")).isEqualTo("\"a");
    assertThat(asConfigPath("\"a.b\"")).isEqualTo("a.b");
  }

  @Test
  public void testStripQuotes() {
    assertThat(stripQuotes(null)).isNull();
    assertThat(stripQuotes("")).isEqualTo("");
    assertThat(stripQuotes("a")).isEqualTo("a");
    assertThat(stripQuotes("\"")).isEqualTo("\"");
    assertThat(stripQuotes("\"a")).isEqualTo("\"a");
    assertThat(stripQuotes("\"ab\"")).isEqualTo("ab");
  }

  @Test
  public void testGettersWithConfigKey() {
    Config config =
        ConfigFactory.parseString("{\n  \"foo.bar\":\"1\"\n\"quux.baz\": {\n    x:y\n  }\n}");
    try {
      config.getString("foo.bar");
      fail("Found foo.bar unexpectedly.");
    } catch (ConfigException.Missing ignore) {
    }
    assertThat(config.getString(asConfigKey("foo.bar"))).isEqualTo("1");
    assertThat(config.getConfig(asConfigKey("quux.baz"))
        .getString(asConfigKey("x"))).isEqualTo("y");
  }

  @Test
  public void testGettersWithConfigPath() {
    Config config =
        ConfigFactory.parseString("{\n  quux: {\n    baz: {\n      x:y\n    }\n  }\n}");
    try {
      config.getString("\"quux.baz\"");
      fail("Found \"quux.baz\" unexpectedly.");
    } catch (ConfigException.Missing ignore) {
    }
    assertThat(config.getConfig(asConfigPath("\"quux.baz\""))
        .getString(asConfigKey("x"))).isEqualTo("y");
  }

  @Test
  public void testGettersWithStripQuotes() {
    Config config =
        ConfigFactory.parseString("{\n  quux: {\n    baz: {\n      x:y\n    }\n  }\n}");
    try {
      config.getString("\"quux.baz\"");
      fail("Found \"quux.baz\" unexpectedly.");
    } catch (ConfigException.Missing ignore) {
    }
    assertThat(config.getConfig(stripQuotes("\"quux\"")).
        getConfig(stripQuotes("\"baz\"")).getString(asConfigKey("x"))).isEqualTo("y");
  }

  @Test
  public void testGetStringMap() {
    Config config = ConfigFactory.parseString(
        "{\n  quux: {\n    baz: {\n      x:y\n    },\n    \"w\":z\n  }\n  a: [1, 2]\n  b:3\n}");
    assertThat(getStringMap(config, "quux")).containsOnly(entry("w", "z"));
    assertThat(getStringMap(config, "a")).containsOnly(entry("1", "1"), entry("2", "2"));
    try {
      getStringMap(config, "c");
      fail("Found c unexpectedly.");
    } catch (ConfigException.Missing ignore) {
    }
    try {
      getStringMap(config, "b");
      fail("Expected b to have the wrong type.");
    } catch (ConfigException.WrongType ignore) {
    }
  }
}
