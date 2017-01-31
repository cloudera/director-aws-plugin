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

package com.cloudera.director.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AWSTimeoutsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private AWSTimeouts timeouts;

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testSomeTimeouts() {
    Config config =
        ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                               .put("timeout1", new Long(1L))
                               .put("timeout2", new Integer(2))
                               .put("time.out3", new Long(3L))
                               .put("time.out4", new Integer(4))
                               .build());
    timeouts = new AWSTimeouts(config);

    assertEquals(1L, timeouts.getTimeout("timeout1").get().longValue());
    assertEquals(2L, timeouts.getTimeout("timeout2").get().longValue());
    assertEquals(3L, timeouts.getTimeout("time.out3").get().longValue());
    assertEquals(4L, timeouts.getTimeout("time.out4").get().longValue());
  }

  @Test
  public void testNoTimeouts() {
    Config config = ConfigFactory.empty();
    timeouts = new AWSTimeouts(config);

    assertFalse(timeouts.getTimeout("timeout1").isPresent());
  }

  @Test
  public void testNoTimeoutsNullConfig() {
    timeouts = new AWSTimeouts(null);

    assertFalse(timeouts.getTimeout("timeout1").isPresent());
  }

  @Test
  public void testBadConfiguration() {
    thrown.expect(IllegalArgumentException.class);
    Config config =
        ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                               .put("timeout1", "1")
                               .build());
    new AWSTimeouts(config);
  }

  @Test
  public void testRejectNegativeTimeout() {
    thrown.expect(IllegalArgumentException.class);
    Config config =
        ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                               .put("timeout1", new Long(-1L))
                               .build());
    new AWSTimeouts(config);
  }
}
