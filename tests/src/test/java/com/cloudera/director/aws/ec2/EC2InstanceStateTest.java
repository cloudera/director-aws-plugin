// (c) Copyright 2019 Cloudera, Inc.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.InstanceStateName;
import com.cloudera.director.spi.v2.model.InstanceStatus;

import org.junit.Test;

public class EC2InstanceStateTest {

  private EC2InstanceState state;

  @Test
  public void testFromInstanceStateName() {
    // only checking PENDING for now
    state = EC2InstanceState.fromInstanceStateName(InstanceStateName.Pending);
    assertThat(state.getInstanceStatus()).isEqualTo(InstanceStatus.PENDING);
    assertThat(state.unwrap()).isEqualTo(InstanceStateName.Pending);
  }

  @Test
  public void testFromInstanceStateNameUnknown() {
    state = EC2InstanceState.fromInstanceStateName(null);
    assertThat(state.getInstanceStatus()).isEqualTo(InstanceStatus.UNKNOWN);
    assertThat(state.unwrap()).isNull();
  }

  @Test
  public void testToString() {
    assertThat(EC2InstanceState.fromInstanceStateName(InstanceStateName.Pending).toString())
        .isEqualTo("PENDING (EC2: pending)");
    assertThat(EC2InstanceState.fromInstanceStateName(null).toString())
        .isEqualTo("UNKNOWN (EC2: unknown)");
  }

}
