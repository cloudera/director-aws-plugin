// (c) Copyright 2018 Cloudera, Inc.
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

package com.cloudera.director.aws.ec2.common;

/**
 * Provides constants and utilities for dealing with EC2 exceptions.
 */
public class EC2Exceptions {

  /**
   * Instance ID not found failure string. This may indicate an eventual consistency issue.
   */
  public static final String INVALID_INSTANCE_ID_NOT_FOUND = "InvalidInstanceID.NotFound";

  /**
   * Private constructor to prevent instantiation.
   */
  private EC2Exceptions() {
  }
}
