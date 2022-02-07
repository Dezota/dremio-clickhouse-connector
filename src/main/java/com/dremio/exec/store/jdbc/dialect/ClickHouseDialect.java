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

package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.ClickHouseSchemaFetcherImpl;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;

public final class ClickHouseDialect extends ArpDialect {

    public ClickHouseDialect(ArpYaml yaml) {
        super(yaml);
    }

    @Override
    public CalendarPolicy getCalendarPolicy() {
        return ClickHouseSqlDialect.DEFAULT.getCalendarPolicy();
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        ClickHouseSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
    }

    @Override
    public void unparseDateTimeLiteral(SqlWriter writer,
                                       SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
        ClickHouseSqlDialect.DEFAULT.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }

    @Override
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
                                   SqlNode fetch) {
        ClickHouseSqlDialect.DEFAULT.unparseOffsetFetch(writer, offset, fetch);
    }

    @Override
    public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
        return ClickHouseSqlDialect.DEFAULT.emulateNullDirection(node, nullsFirst, desc);
    }

    @Override
    public SqlNode getCastSpec(RelDataType type) {
        return ClickHouseSqlDialect.DEFAULT.getCastSpec(type);
    }

    @Override
    public JdbcSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
        return new ClickHouseSchemaFetcherImpl(config);
    }
}
