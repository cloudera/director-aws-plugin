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

package com.cloudera.director.aws;

import static org.assertj.core.api.Assertions.assertThat;

import com.cloudera.director.aws.STSAssumeNRolesSessionCredentialsProvider.RoleConfiguration;
import com.cloudera.director.aws.shaded.com.typesafe.config.Config;
import com.cloudera.director.aws.shaded.com.typesafe.config.ConfigFactory;
import com.google.common.base.Function;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class STSRolesTest {
  private static final String ROLE = "roleSwitch";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/" + ROLE;
  private static final String QUOTED_ROLE_ARN = "\"" + ROLE_ARN + "\"";
  private static final String ROLE_SESSION_NAME = "reader";
  private static final String ROLE_EXTERNAL_ID = "token";

  @Rule
  public TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testParseConfigWithArnSessionNameAndExternalId() throws IOException {
    Function<PrintWriter, Void> writer = new Function<PrintWriter, Void>() {
      @Override
      public Void apply(PrintWriter printWriter) {
        printWriter.println("  {");
        printWriter.println("    " + STSRoles.ROLE_ARN + ": " + QUOTED_ROLE_ARN);
        printWriter.println("    " + STSRoles.ROLE_SESSION_NAME + ": " + ROLE_SESSION_NAME);
        printWriter.println("    " + STSRoles.ROLE_EXTERNAL_ID + ": " + ROLE_EXTERNAL_ID);
        printWriter.println("  }");
        return null;
      }
    };

    Function<STSRoles, Void> assertions = new Function<STSRoles, Void>() {
      @Override
      public Void apply(STSRoles stsRoles) {
        assertThat(stsRoles.getRoleConfigurations()).isNotNull();
        assertThat(stsRoles.getRoleConfigurations().size()).isEqualTo(1);

        RoleConfiguration roleConfiguration = stsRoles.getRoleConfigurations().get(0);
        assertThat(roleConfiguration.getRoleArn()).isEqualTo(ROLE_ARN);
        assertThat(roleConfiguration.getRoleSessionName()).isEqualTo(ROLE_SESSION_NAME);
        assertThat(roleConfiguration.getRoleExternalId()).isEqualTo(ROLE_EXTERNAL_ID);
        return null;
      }
    };

    testWithFile(writer, assertions);
  }

  @Test
  public void testParseConfigWithArnAndSessionName() throws IOException {
    Function<PrintWriter, Void> writer = new Function<PrintWriter, Void>() {
      @Override
      public Void apply(PrintWriter printWriter) {
        printWriter.println("  {");
        printWriter.println("    " + STSRoles.ROLE_ARN + ": " + QUOTED_ROLE_ARN);
        printWriter.println("    " + STSRoles.ROLE_SESSION_NAME + ": " + ROLE_SESSION_NAME);
        printWriter.println("  }");
        return null;
      }
    };

    Function<STSRoles, Void> assertions = new Function<STSRoles, Void>() {
      @Override
      public Void apply(STSRoles stsRoles) {
        assertThat(stsRoles.getRoleConfigurations()).isNotNull();
        assertThat(stsRoles.getRoleConfigurations().size()).isEqualTo(1);

        RoleConfiguration roleConfiguration = stsRoles.getRoleConfigurations().get(0);
        assertThat(roleConfiguration.getRoleArn()).isEqualTo(ROLE_ARN);
        assertThat(roleConfiguration.getRoleSessionName()).isEqualTo(ROLE_SESSION_NAME);
        assertThat(roleConfiguration.getRoleExternalId()).isNull();
        return null;
      }
    };

    testWithFile(writer, assertions);
  }

  @Test
  public void testParseConfigWithArn() throws IOException {
    Function<PrintWriter, Void> writer = new Function<PrintWriter, Void>() {
      @Override
      public Void apply(PrintWriter printWriter) {
        printWriter.println("  {");
        printWriter.println("    " + STSRoles.ROLE_ARN + ": " + QUOTED_ROLE_ARN);
        printWriter.println("  }");
        return null;
      }
    };

    Function<STSRoles, Void> assertions = new Function<STSRoles, Void>() {
      @Override
      public Void apply(STSRoles stsRoles) {
        assertThat(stsRoles.getRoleConfigurations()).isNotNull();
        assertThat(stsRoles.getRoleConfigurations().size()).isEqualTo(1);

        RoleConfiguration roleConfiguration = stsRoles.getRoleConfigurations().get(0);
        assertThat(roleConfiguration.getRoleArn()).isEqualTo(ROLE_ARN);
        assertThat(roleConfiguration.getRoleSessionName()).isEqualTo(ROLE);
        assertThat(roleConfiguration.getRoleExternalId()).isNull();
        return null;
      }
    };

    testWithFile(writer, assertions);
  }

  @Test
  public void testEmpty() throws IOException {
    Function<PrintWriter, Void> writer = new Function<PrintWriter, Void>() {
      @Override
      public Void apply(PrintWriter printWriter) {
        return null;
      }
    };
    Function<STSRoles, Void> assertions = new Function<STSRoles, Void>() {
      @Override
      public Void apply(STSRoles stsRoles) {
        assertThat(stsRoles.getRoleConfigurations()).isEmpty();
        return null;
      }
    };

    testWithFile(writer, assertions);
  }

  @Test
  public void testParseArray() throws IOException {
    Function<PrintWriter, Void> writer = new Function<PrintWriter, Void>() {
      @Override
      public Void apply(PrintWriter printWriter) {
        printWriter.println("  {");
        printWriter.println("    " + STSRoles.ROLE_ARN + ": " + QUOTED_ROLE_ARN);
        printWriter.println("    " + STSRoles.ROLE_SESSION_NAME + ": " + ROLE_SESSION_NAME);
        printWriter.println("    " + STSRoles.ROLE_EXTERNAL_ID + ": " + ROLE_EXTERNAL_ID);
        printWriter.println("  },");
        printWriter.println("  {");
        printWriter.println("    " + STSRoles.ROLE_ARN + ": " + QUOTED_ROLE_ARN);
        printWriter.println("    " + STSRoles.ROLE_SESSION_NAME + ": " + ROLE_SESSION_NAME);
        printWriter.println("  },");
        return null;
      }
    };

    Function<STSRoles, Void> assertions = new Function<STSRoles, Void>() {
      @Override
      public Void apply(STSRoles stsRoles) {
        assertThat(stsRoles.getRoleConfigurations()).isNotNull();
        assertThat(stsRoles.getRoleConfigurations().size()).isEqualTo(2);

        RoleConfiguration roleConfiguration = stsRoles.getRoleConfigurations().get(0);
        assertThat(roleConfiguration.getRoleArn()).isEqualTo(ROLE_ARN);
        assertThat(roleConfiguration.getRoleSessionName()).isEqualTo(ROLE_SESSION_NAME);
        assertThat(roleConfiguration.getRoleExternalId()).isEqualTo(ROLE_EXTERNAL_ID);

        roleConfiguration = stsRoles.getRoleConfigurations().get(1);
        assertThat(roleConfiguration.getRoleArn()).isEqualTo(ROLE_ARN);
        assertThat(roleConfiguration.getRoleSessionName()).isEqualTo(ROLE_SESSION_NAME);
        assertThat(roleConfiguration.getRoleExternalId()).isNull();
        return null;
      }
    };

    testWithFile(writer, assertions);
  }

  private void testWithFile(Function<PrintWriter, Void> writer, Function<STSRoles, Void> assertions)
      throws IOException {
    File configDir = TEMPORARY_FOLDER.getRoot();
    File configFile = new File(configDir, Configurations.CONFIGURATION_FILE_NAME);

    try (OutputStream ostream = new FileOutputStream(configFile)) {
      try (OutputStreamWriter owriter = new OutputStreamWriter(ostream, "UTF-8")) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(owriter)) {
          try (PrintWriter printWriter = new PrintWriter(bufferedWriter)) {
            printWriter.println(Configurations.STS_ROLES_SECTION + ": [");
            writer.apply(printWriter);
            printWriter.println("]");
            printWriter.close();

            Config config = ConfigFactory.parseFile(configFile);
            STSRoles stsRoles = new STSRoles(config.getConfigList(Configurations.STS_ROLES_SECTION));
            assertions.apply(stsRoles);
          }
        }
      }
    }
  }
}
