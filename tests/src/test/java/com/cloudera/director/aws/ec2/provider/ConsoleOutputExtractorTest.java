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

package com.cloudera.director.aws.ec2.provider;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.Test;

public class ConsoleOutputExtractorTest {

  private final ConsoleOutputExtractor consoleOutputExtractor = new ConsoleOutputExtractor();

  private static final String VALID_OUTPUT = "ec2: #############################################################\n" +
      "\n" +
      "ec2: -----BEGIN SSH HOST KEY FINGERPRINTS-----\n" +
      "\n" +
      "ec2: 1024 6d:99:6d:f1:d5:42:42:68:f1:5b:40:e9:ff:30:82:38 /etc/ssh/ssh_host_dsa_key.pub (DSA)\n" +
      "\n" +
      "ec2: 2048 2d:e1:d3:48:06:0d:32:32:1b:14:3a:87:49:18:ca:2a /etc/ssh/ssh_host_key.pub (RSA1)\n" +
      "\n" +
      "ec2: 2048 7f:1b:3b:51:42:2e:4e:be:9f:f1:77:15:a6:33:62:c7 /etc/ssh/ssh_host_rsa_key.pub (RSA)\n" +
      "\n" +
      "ec2: -----END SSH HOST KEY FINGERPRINTS-----\n" +
      "\n" +
      "ec2: #############################################################\n";

  private static final String EMPTY_FINGERPRINTS_BLOCK = "ec2: #############################################################\n" +
      "\n" +
      "ec2: -----BEGIN SSH HOST KEY FINGERPRINTS-----\n" +
      "ec2: -----END SSH HOST KEY FINGERPRINTS-----\n" +
      "\n" +
      "ec2: #############################################################\n";

  @Test
  public void getHostKeyFingerprintsTests() {
    assertThat(consoleOutputExtractor.hasHostKeyFingerprintBlock(VALID_OUTPUT)).isTrue();
    Set<String> hostKeyFingerprints = consoleOutputExtractor.getHostKeyFingerprints(VALID_OUTPUT);
    assertThat(hostKeyFingerprints).contains(
        "6d:99:6d:f1:d5:42:42:68:f1:5b:40:e9:ff:30:82:38",
        "2d:e1:d3:48:06:0d:32:32:1b:14:3a:87:49:18:ca:2a",
        "7f:1b:3b:51:42:2e:4e:be:9f:f1:77:15:a6:33:62:c7"
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void getHostKeyFingerprintsTests_IllegalArgument() {
    String consoleOutput = "asdf";
    assertThat(consoleOutputExtractor.hasHostKeyFingerprintBlock(consoleOutput)).isFalse();
    consoleOutputExtractor.getHostKeyFingerprints(consoleOutput);
  }

  @Test
  public void getHostKeyFingerprintsTests_EmptyBlock() {
    assertThat(consoleOutputExtractor.hasHostKeyFingerprintBlock(EMPTY_FINGERPRINTS_BLOCK)).isTrue();
    Set<String> fingerprints = consoleOutputExtractor.getHostKeyFingerprints(EMPTY_FINGERPRINTS_BLOCK);
    assertThat(fingerprints).isEmpty();
  }
}
