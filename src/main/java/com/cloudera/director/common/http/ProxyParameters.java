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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Optional;

/**
 * Houses and validates proxy parameters.
 */
public class ProxyParameters {
  private final Optional<String> host;
  private final int port;
  private final Optional<String> username;
  private final Optional<String> password;
  private final Optional<String> domain;
  private final Optional<String> workstation;
  private final boolean preemptiveBasicProxyAuth;

  /**
   * Constructs the object and validates the parameters.
   * <p/>
   * The host and port must be specified together.
   * <p/>
   * The username and password must also be specified together and must accompany the
   * host and port.
   * <p/>
   * The domain and workstation must be specified together and must accompany the
   * username, password, host, and port.
   *
   * @param host                     The proxy host.
   * @param port                     The proxy port.
   * @param username                 The proxy username.
   * @param password                 The proxy password.
   * @param domain                   The proxy domain (NTLM authentication only).
   * @param workstation              The proxy workstation (NTLM authentication only).
   * @param preemptiveBasicProxyAuth Whether the proxy should preemptively authenticate.
   */
  @SuppressWarnings("PMD.UselessParentheses")
  public ProxyParameters(String host, int port, String username,
      String password, String domain, String workstation,
      boolean preemptiveBasicProxyAuth) {
    this.host = Optional.fromNullable(host);
    this.port = port;

    checkArgument(!this.host.isPresent() || (this.port > 0 && this.port < 65536),
        "The supplied port must be a positive number less than 65536");

    this.username = Optional.fromNullable(username);
    this.password = Optional.fromNullable(password);

    checkArgument(this.username.isPresent() == this.password.isPresent(),
        "Both the proxy username and password must be supplied");
    checkArgument(this.host.isPresent() || !this.username.isPresent(),
        "A host and port must be supplied with a username and password");

    this.domain = Optional.fromNullable(domain);
    this.workstation = Optional.fromNullable(workstation);

    checkArgument(this.domain.isPresent() == this.workstation.isPresent(),
        "Both the proxy domain and workstation must be supplied");
    checkArgument(this.username.isPresent() || !this.domain.isPresent(),
        "NTLM requires a username, password, domain, and workstation to be supplied.");

    this.preemptiveBasicProxyAuth = preemptiveBasicProxyAuth;
  }

  /**
   * Returns the proxy host.
   *
   * @return The proxy host or Optional.absent() if not set.
   */
  public Optional<String> getHost() {
    return host;
  }

  /**
   * Returns the proxy port.
   *
   * @return The proxy port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the proxy user name.
   *
   * @return The proxy user name or Optional.absent() if not set.
   */
  public Optional<String> getUsername() {
    return username;
  }

  /**
   * Return the proxy password.
   *
   * @return The proxy password or Optional.absent() if not set.
   */
  public Optional<String> getPassword() {
    return password;
  }

  /**
   * Returns the proxy domain (only used in NTLM authentication).
   *
   * @return The proxy domain or Optional.absent() if not set.
   */
  public Optional<String> getDomain() {
    return domain;
  }

  /**
   * Returns the proxy workstation (only used in NTLM authentication).
   *
   * @return The proxy workstation or Optional.absent() if not set.
   */
  public Optional<String> getWorkstation() {
    return workstation;
  }

  /**
   * Returns true if a proxy should use preemptive basic authentication.
   *
   * @return True if the proxy should use preemptive basic authentication.
   */
  public boolean isPreemptiveBasicProxyAuth() {
    return preemptiveBasicProxyAuth;
  }
}
