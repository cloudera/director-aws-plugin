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

package com.cloudera.director.common.http;

import org.junit.Test;

public class ProxyParametersTest {

  @Test
  public void testValidProxyConstructor() {
    new ProxyParameters("host", 2, "user", "pass", "domain", "workstation", false);
    new ProxyParameters("host", 2, "user", "pass", null, null, false);
    new ProxyParameters("host", 2, null, null, null, null, false);
    new ProxyParameters(null, -1, null, null, null, null, false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testProxyConstructorBadPort() {
    new ProxyParameters("host", -1, "user", "pass", "domain", "workstation", false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testProxyConstructorMissingUser() {
    new ProxyParameters("host", 1234, null, "pass", "domain", "workstation", false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testProxyConstructorMissingPassword() {
    new ProxyParameters("host", 1234, "user", null, "domain", "workstation", false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testProxyConstructorMissingDomain() {
    new ProxyParameters("host", 1234, "user", "pass", null, "workstation", false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testProxyConstructorMissingWorkstation() {
    new ProxyParameters("host", 1234, "user", "pass", "domain", null, false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testProxyConstructorSuppliedUserMissingHost() {
    new ProxyParameters(null, 1234, "user", "pass", "domain", "workstation", false);
  }
}
