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

package com.cloudera.director.aws.rds;

import static org.assertj.core.api.Assertions.assertThat;

import com.cloudera.director.aws.rds.RDSInstanceTemplate.RDSInstanceTemplateConfigurationPropertyToken;
import com.cloudera.director.spi.v1.database.DatabaseServerInstanceTemplate.DatabaseServerInstanceTemplateConfigurationPropertyToken;
import com.cloudera.director.spi.v1.database.DatabaseType;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;

import org.junit.Test;

public class RDSInstanceTemplateTest {

  private static final String NAME = "testTemplate";
  private static final Map<String, String> TAGS = ImmutableMap.of();
  private static final LocalizationContext LOCALIZATION_CONTEXT =
      DefaultLocalizationContext.FACTORY.createRootLocalizationContext(Locale.US);

  private SimpleConfiguration configuration;
  private RDSInstanceTemplate t;

  private static String asKey(ConfigurationPropertyToken token) {
    return token.unwrap().getConfigKey();
  }

  @Test
  public void testGetters() {
    configuration = new SimpleConfiguration(ImmutableMap.<String, String>builder()

        // required for DatabaseServerInstanceTemplate
        .put(asKey(DatabaseServerInstanceTemplateConfigurationPropertyToken.TYPE), DatabaseType.MYSQL.toString())
        // ADMIN_USERNAME and ADMIN_PASSWORD are handled with MASTER_USERNAME
        // and MASTER_USER_PASSWORD below

        // required for RDSInstanceTemplate
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.ALLOCATED_STORAGE), "50")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.DB_SUBNET_GROUP_NAME), "default")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.INSTANCE_CLASS), "db.t2.micro")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.VPC_SECURITY_GROUP_IDS), "sg-1,sg-2")

        // optional for RDSInstanceTemplate
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.AUTO_MINOR_VERSION_UPGRADE), "false")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.AVAILABILITY_ZONE), "us-east-1x")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.BACKUP_RETENTION_PERIOD), "42")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.DB_NAME), "myDb")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.DB_PARAMETER_GROUP_NAME), "myDbParams")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.ENGINE), RDSEngine.MYSQL.getEngineName())
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.ENGINE_VERSION), "5.6.123")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.LICENSE_MODEL), "open")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.MASTER_USERNAME), "root")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.MASTER_USER_PASSWORD), "director")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.MULTI_AZ), "true")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.OPTION_GROUP_NAME), "myOptions")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.PORT), "3307")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.PREFERRED_BACKUP_WINDOW), "2am")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.PREFERRED_MAINTENANCE_WINDOW), "3am")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.PUBLICLY_ACCESSIBLE), "true")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.SKIP_FINAL_SNAPSHOT), "true")
        .put(asKey(RDSInstanceTemplateConfigurationPropertyToken.STORAGE_ENCRYPTED), "true")

        .build());
    t = new RDSInstanceTemplate(NAME, configuration, TAGS, LOCALIZATION_CONTEXT);

    assertThat(t.getDatabaseType()).isEqualTo(DatabaseType.MYSQL);
    assertThat(t.getAdminUser()).isEqualTo("root");
    assertThat(t.getAdminPassword()).isEqualTo("director");

    assertThat(t.getAllocatedStorage()).isEqualTo(50);
    assertThat(t.getDbSubnetGroupName()).isEqualTo("default");
    assertThat(t.getInstanceClass()).isEqualTo("db.t2.micro");
    assertThat(t.getVpcSecurityGroupIds()).containsExactly("sg-1", "sg-2");

    assertThat(t.getAutoMinorVersionUpgrade().get()).isFalse();
    assertThat(t.getAvailabilityZone().get()).isEqualTo("us-east-1x");
    assertThat(t.getBackupRetentionPeriod().get()).isEqualTo(42);
    assertThat(t.getDbName().get()).isEqualTo("myDb");
    assertThat(t.getDbParameterGroupName().get()).isEqualTo("myDbParams");
    assertThat(t.getEngine()).isEqualTo(RDSEngine.MYSQL.getEngineName());
    assertThat(t.getEngineVersion().get()).isEqualTo("5.6.123");
    assertThat(t.getLicenseModel().get()).isEqualTo("open");
    assertThat(t.getMasterUsername().get()).isEqualTo("root");
    assertThat(t.getMasterUserPassword().get()).isEqualTo("director");
    assertThat(t.isMultiAZ().get()).isTrue();
    assertThat(t.getOptionGroupName().get()).isEqualTo("myOptions");
    assertThat(t.getPort().get()).isEqualTo(3307);
    assertThat(t.getPreferredBackupWindow().get()).isEqualTo("2am");
    assertThat(t.getPreferredMaintenanceWindow().get()).isEqualTo("3am");
    assertThat(t.isPubliclyAccessible().get()).isTrue();
    assertThat(t.isSkipFinalSnapshot().get()).isTrue();
    assertThat(t.isStorageEncrypted().get()).isTrue();
  }

}
