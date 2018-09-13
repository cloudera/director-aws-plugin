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

package com.cloudera.director.aws.ec2;

import com.amazonaws.services.ec2.model.Tag;
import com.cloudera.director.aws.AbstractAWSTagHelper;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;

import java.util.Map;

/**
 * Helper class for creating EC2 tags with custom tag names.
 */
public class EC2TagHelper extends AbstractAWSTagHelper<Tag> {

  /**
   * The maximum number of allowed user-defined tags.
   */
  public static final int MAX_TAGS_ALLOWED =
      50 - Tags.InstanceTags.values().length - Tags.ResourceTags.values().length;

  /**
   * Creates an EC2 tag helper with the specified parameters.
   *
   * @param customTagMappings the custom tag mappings
   */
  public EC2TagHelper(CustomTagMappings customTagMappings) {
    super(customTagMappings);
  }

  /**
   * Creates an implementation-specific tag with the specified key and value.
   *
   * @param tagKey   the tag key
   * @param tagValue the tag value
   * @return an implementation-specific tag with the specified key and value
   */
  @Override
  public Tag createTagImpl(String tagKey, String tagValue) {
    return new Tag(tagKey, tagValue);
  }


  /**
   * Validate tags input.
   * A null tags map is allowed.
   * Number of entries should not exceed {@link #MAX_TAGS_ALLOWED}.
   *
   * @param tags given map of tags
   */
  public void validateTags(Map<String, String> tags) {
    if (tags != null && tags.size() > MAX_TAGS_ALLOWED) {
      throw new UnrecoverableProviderException("Number of tags exceeds the maximum of " +
          MAX_TAGS_ALLOWED);
    }
  }
}
