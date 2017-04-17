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

import static org.junit.Assert.assertEquals;

import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CustomTagMappingsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testSomeCustomTagMappings() {
    Config config =
        ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                               .put("tag1", "t1")
                               .put("tag2", "t2")
                               .put("t.ag3", "t3")
                               .put("t.ag4", "t4")
                               .build());
    CustomTagMappings customTagMappings = new CustomTagMappings(config);

    assertEquals("t1", customTagMappings.getCustomTagName("tag1"));
    assertEquals("t2", customTagMappings.getCustomTagName("tag2"));
    assertEquals("t3", customTagMappings.getCustomTagName("t.ag3"));
    assertEquals("t4", customTagMappings.getCustomTagName("t.ag4"));
  }

  @Test
  public void testNoCustomTagMappings() {
    Config config = ConfigFactory.empty();
    CustomTagMappings customTagMappings = new CustomTagMappings(config);

    assertEquals("tag1", customTagMappings.getCustomTagName("tag1"));
  }

  @Test
  public void testNoCustomTagMappingNullConfig() {
    CustomTagMappings customTagMappings = new CustomTagMappings(null);

    assertEquals("tag1", customTagMappings.getCustomTagName("tag1"));
  }

  @Test
  public void testBadConfiguration() {
    thrown.expect(IllegalArgumentException.class);
    Config config =
        ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                               .put("tag1", 1)
                               .build());
    new CustomTagMappings(config);
  }

  @Test
  public void testGetClouderaDirectorIdTagName() {
    Config config =
        ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "foo")
            .build());
    CustomTagMappings customTagMappings = new CustomTagMappings(config);
    assertEquals("foo", customTagMappings.getClouderaDirectorIdTagName());
  }
}
