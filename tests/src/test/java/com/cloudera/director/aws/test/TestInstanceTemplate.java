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

package com.cloudera.director.aws.test;

import static com.cloudera.director.spi.v2.model.InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX;

import com.cloudera.director.aws.Tags;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Convenience class to store instance template configs and tags.
 */
public class TestInstanceTemplate {

  private String templateName;
  private Map<String, String> configs;
  private Map<String, String> tags;

  public void addConfig(ConfigurationPropertyToken propertyToken, String value) {
    if (value != null) {
      configs.put(propertyToken.unwrap().getConfigKey(), value);
    }
  }

  public void addTag(String tagKey, String tagValue) {
    tags.put(tagKey, tagValue);
  }

  /**
   * Constructor, will set an owner tag and instance name prefix based
   * on the current user name.
   */
  public TestInstanceTemplate() {
    configs = new LinkedHashMap<>();
    tags = new LinkedHashMap<>();

    String username = System.getProperty("user.name");
    templateName = username + "-test";

    addConfig(INSTANCE_NAME_PREFIX, templateName);
    addTag(Tags.InstanceTags.OWNER.getTagKey(), username);
  }

  public String getTemplateName() {
    return templateName;
  }

  public Map<String, String> getConfigs() {
    return configs;
  }

  public Map<String, String> getTags() {
    return tags;
  }
}
