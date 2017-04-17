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

package com.cloudera.director.aws.rds;

import com.amazonaws.services.rds.model.Tag;
import com.cloudera.director.aws.AbstractAWSTagHelper;
import com.cloudera.director.aws.CustomTagMappings;

/**
 * Helper class for creating RDS tags with custom tag names.
 */
public class RDSTagHelper extends AbstractAWSTagHelper<Tag> {

  /**
   * Creates an RDS tag helper with the specified parameters.
   *
   * @param customTagMappings the custom tag mappings
   */
  public RDSTagHelper(CustomTagMappings customTagMappings) {
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
    return new Tag().withKey(tagKey).withValue(tagValue);
  }
}
