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

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains functions to extract information from EC2 Console Output.
 */
final class ConsoleOutputExtractor {

  private static final Pattern FINGERPRINTS_BLOCK_PATTERN = Pattern.compile(
      "(BEGIN SSH HOST KEY FINGERPRINTS)(.*)(END SSH HOST KEY FINGERPRINTS)", Pattern.DOTALL
  );

  private static final Pattern FINGERPRINT_PATTERN =
      Pattern.compile("\\s(([0-9a-f]{2}:){15}[0-9a-f]{2})\\s");

  /**
   * Returns whether the ec2 console output contains a host key fingerprint section.
   *
   * @param consoleOutput the ec2 console output
   * @return whether a host key fingerprint section is present
   */
  boolean hasHostKeyFingerprintBlock(String consoleOutput) {
    Matcher match = FINGERPRINTS_BLOCK_PATTERN.matcher(consoleOutput);
    return match.find();
  }

  /**
   * Get the host key fingerprints from the console output. Assumes there
   * is a host key fingerprint block in the console output.
   *
   * @param consoleOutput the ec2 console output
   * @return a set of host key fingerprints
   */
  Set<String> getHostKeyFingerprints(String consoleOutput) {
    Matcher match = FINGERPRINTS_BLOCK_PATTERN.matcher(consoleOutput);
    if (!match.find()) {
      throw new IllegalArgumentException("No SSH Host Key Fingerprint section in console output");
    }
    String fingerprintBlock = match.group(2);

    Set<String> hostKeyFingerprints = Sets.newHashSet();
    match = FINGERPRINT_PATTERN.matcher(fingerprintBlock);
    while (match.find()) {
      hostKeyFingerprints.add(match.group(1));
    }
    return hostKeyFingerprints;
  }
}
