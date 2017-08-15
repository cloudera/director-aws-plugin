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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.spi.v1.model.InstanceTemplate;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class AbstractAWSTagHelperTest {

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testConstructor() {
    CustomTagMappings customTagMappings = mock(CustomTagMappings.class);
    assertEquals(
        customTagMappings,
        new TestAWSTagHelper(customTagMappings).getCustomTagMappings());
  }

  @Test
  public void testGetClouderaDirectorIdTagName() {
    assertEquals(
        Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(),
        new TestAWSTagHelper(new CustomTagMappings(null)).getClouderaDirectorIdTagName());

    CustomTagMappings customTagMappings = mock(CustomTagMappings.class);
    when(customTagMappings.getClouderaDirectorIdTagName())
        .thenReturn("foo");
    assertEquals(
        "foo",
        new TestAWSTagHelper(customTagMappings).getClouderaDirectorIdTagName());
  }

  @Test
  public void testCreateClouderaDirectorIdTag() {
    assertEquals(
        createTestTag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "bar"),
        new TestAWSTagHelper(new CustomTagMappings(null)).createClouderaDirectorIdTag("bar"));

    CustomTagMappings customTagMappings = mock(CustomTagMappings.class);
    when(customTagMappings.getCustomTagName(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey()))
        .thenReturn("foo");

  assertEquals(
      createTestTag("foo", "bar"),
      new TestAWSTagHelper(customTagMappings).createClouderaDirectorIdTag("bar"));
  }

  @Test
  public void testCreateClouderaDirectorTemplateNameTag() {
    assertEquals(
        createTestTag(Tags.ResourceTags.CLOUDERA_DIRECTOR_TEMPLATE_NAME.getTagKey(), "bar"),
        new TestAWSTagHelper(new CustomTagMappings(null)).createClouderaDirectorTemplateNameTag("bar"));

    CustomTagMappings customTagMappings = mock(CustomTagMappings.class);
    when(customTagMappings.getCustomTagName(Tags.ResourceTags.CLOUDERA_DIRECTOR_TEMPLATE_NAME.getTagKey()))
        .thenReturn("foo");
    assertEquals(
        createTestTag("foo", "bar"),
        new TestAWSTagHelper(customTagMappings).createClouderaDirectorTemplateNameTag("bar"));
  }

  @Test
  public void testGetUserDefinedTags() {
    Map<String, String> instanceTemplateTags = ImmutableMap.<String, String>builder()
        .put("t1", "v1")
        .put("t2", "v2")
        .build();
    InstanceTemplate instanceTemplate = mock(InstanceTemplate.class);
    when(instanceTemplate.getTags()).thenReturn(instanceTemplateTags);

    CustomTagMappings customTagMappings = mock(CustomTagMappings.class);
    when(customTagMappings.getCustomTagName("t1"))
        .thenReturn("c1");
    when(customTagMappings.getCustomTagName("t2"))
        .thenReturn("c2");

    TestAWSTagHelper tagHelper = new TestAWSTagHelper(customTagMappings);
    Set<String> userDefinedTags = new HashSet<>(tagHelper.getUserDefinedTags(instanceTemplate));
    assertEquals(new HashSet<>(Arrays.asList(createTestTag("t1", "v1"), createTestTag("t2", "v2"))),
        userDefinedTags);
  }

  @Test
  public void testGetInstanceTags() {
    Map<String, String> instanceTemplateTags = ImmutableMap.<String, String>builder()
        .put("t1", "v1")
        .put("t2", "v2")
        .build();
    InstanceTemplate instanceTemplate = mock(InstanceTemplate.class);
    when(instanceTemplate.getTags()).thenReturn(instanceTemplateTags);
    when(instanceTemplate.getName()).thenReturn("name");
    when(instanceTemplate.getInstanceNamePrefix()).thenReturn("p");

    CustomTagMappings customTagMappings = mock(CustomTagMappings.class);
    when(customTagMappings.getCustomTagName("t1"))
        .thenReturn("c1");
    when(customTagMappings.getCustomTagName("t2"))
        .thenReturn("t2");
    when(customTagMappings.getCustomTagName(Tags.ResourceTags.RESOURCE_NAME.getTagKey()))
        .thenReturn(Tags.ResourceTags.RESOURCE_NAME.getTagKey());
    when(customTagMappings.getCustomTagName(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey()))
        .thenReturn(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey());
    when(customTagMappings.getCustomTagName(Tags.ResourceTags.CLOUDERA_DIRECTOR_TEMPLATE_NAME.getTagKey()))
        .thenReturn(Tags.ResourceTags.CLOUDERA_DIRECTOR_TEMPLATE_NAME.getTagKey());

    TestAWSTagHelper tagHelper = new TestAWSTagHelper(customTagMappings);
    List<String> userDefinedTags = tagHelper.getUserDefinedTags(instanceTemplate);
    Set<String> instanceTags =
        new HashSet<>(tagHelper.getInstanceTags(instanceTemplate, "id", userDefinedTags));
    Set<String> expectedInstanceTags = new HashSet<>(Arrays.asList(
            createTestTag(Tags.ResourceTags.RESOURCE_NAME.getTagKey(), "p-id"),
            createTestTag(Tags.ResourceTags.CLOUDERA_DIRECTOR_ID.getTagKey(), "id"),
            createTestTag(Tags.ResourceTags.CLOUDERA_DIRECTOR_TEMPLATE_NAME.getTagKey(), "name")));
    expectedInstanceTags.addAll(userDefinedTags);
    assertEquals(expectedInstanceTags, instanceTags);
  }

  @Test(expected = NullPointerException.class)
  public void testNullCustomMappings() {
    new TestAWSTagHelper(null);
  }

  public static String createTestTag(String tagKey, String tagValue) {
    return tagKey + "." + tagValue;
  }

  private static class TestAWSTagHelper extends AbstractAWSTagHelper<String> {

    private TestAWSTagHelper(CustomTagMappings customTagMappings) {
      super(customTagMappings);
    }

    @Override
    public String createTagImpl(String tagKey, String tagValue) {
      return createTestTag(tagKey, tagValue);
    }
  }
}
