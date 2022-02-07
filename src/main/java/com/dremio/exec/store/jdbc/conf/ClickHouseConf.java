/*
 * Copyright (C) 2017-2021 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 *
 * @summary Created a Dremio Connector for Clickhouse with the updated API for Dremio 19.1.0 with inspiration from:
 *    https://www.dremio.com/resources/tutorials/how-to-create-an-arp-connector/
 *    https://github.com/altxcorp/dremio-clickhouse-arp-connector
 *    https://github.com/dremio-hub/dremio-sqllite-connector
 *
 * @author Brian Holman <bholman@dezota.com>
 *
 */

package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.exec.store.jdbc.dialect.ClickHouseDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.dremio.exec.catalog.conf.Secret;

import io.protostuff.Tag;

/**
 * Configuration for ClickHouse sources.
 */

@SourceType(value = "CLICKHOUSE", label = "ClickHouse", uiConfig = "clickhouse-layout.json", externalQuerySupported = true)
public class ClickHouseConf extends AbstractArpConf<ClickHouseConf> {
  private static final String ARP_FILENAME = "arp/implementation/clickhouse-arp.yaml";
  //private static final ArpDialect CLICKHOUSE_ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final ClickHouseDialect CLICKHOUSE_ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, ClickHouseDialect::new);
  private static final String DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Host [localhost, 127.0.0.1, 127.1.1.0]")
  public String hostname="localhost";

  @NotBlank
  @Tag(2)
  @DisplayMetadata(label = "Port [8123]")
  public String port="8123";

  @Tag(3)
  @DisplayMetadata(label = "Database [default]")
  public String database="default";

  @NotBlank
  @Tag(4)
  @DisplayMetadata(label = "User [default]")
  public String username="default";

  @NotBlank
  @Secret
  @Tag(5)
  @DisplayMetadata(label = "Password")
  public String password;

  @Tag(7)
  @DisplayMetadata(label = "Maximum idle connections")
  @NotMetadataImpacting
  public int maxIdleConns = 8;

  @Tag(8)
  @DisplayMetadata(label = "Connection idle time (s)")
  @NotMetadataImpacting
  public int idleTimeSec = 60;

  @Tag(9)
  @NotMetadataImpacting
  @JsonIgnore
  public boolean enableExternalQuery = true;
  
  @VisibleForTesting
  public String toJdbcConnectionString() {
    hostname = hostname == null ? "localhost" : hostname;
    port = port == null ? "8123" : port;
    database = database == null ? "default" : database;
    username = username == null ? "default" : username;
    final String password = checkNotNull(this.password, "Missing Password.");

    // jdbc:clickhouse://<host>:<port>[/<database>]
    // jdbc:clickhouse://<host>:<port>[/<database>]?user=<username>&password=<password>

    return String.format("jdbc:clickhouse://%s:%s/%s?user=%s&password=%s", hostname, port, database, username, password );
  }

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(
          JdbcPluginConfig.Builder configBuilder,
          CredentialsService credentialsService,
          OptionManager optionManager
  ) {
    return configBuilder.withDialect(getDialect())
            .withDialect(getDialect())
            .withDatasourceFactory(this::newDataSource)
            .clearHiddenSchemas()
            .addHiddenSchema("INFORMATION_SCHEMA")
            .addHiddenSchema("system")
            .addHiddenTableType("FOREIGN TABLE")
            .addHiddenTableType("SYSTEM VIEW")
            .build();
  }

  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
            toJdbcConnectionString(), null, null, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE,
            maxIdleConns, idleTimeSec);
  }

  @Override
  public ClickHouseDialect getDialect() {
    return CLICKHOUSE_ARP_DIALECT;
  }

  @VisibleForTesting
  public static ClickHouseDialect getDialectSingleton() {
    return CLICKHOUSE_ARP_DIALECT;
  }
}
