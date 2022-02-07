/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.RelToSqlConverterUtil;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlDataTypeSpec;

import java.util.TimeZone;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlDialect</code> implementation for the ClickHouse database.
 */
public class ClickHouseSqlDialect extends SqlDialect {
    public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
            //.withDatabaseProduct(SqlDialect.DatabaseProduct.CLICKHOUSE) // Backported minimal calcite ClickHouse SQL Dialect
            .withIdentifierQuoteString("`")
            .withNullCollation(NullCollation.LOW);

    public static final SqlDialect DEFAULT = new ClickHouseSqlDialect(DEFAULT_CONTEXT);

    /**
     * Creates a ClickHouseSqlDialect.
     */
    public ClickHouseSqlDialect(Context context) {
        super(context);
    }

    @Override
    public boolean supportsCharSet() {
        return false;
    }

    @Override
    public boolean supportsNestedAggregations() {
        return false;
    }

    public boolean supportsWindowFunctions() {
        return false;
    }

    @Override
    public boolean supportsAliasedValues() {
        return false;
    }

    @Override
    public CalendarPolicy getCalendarPolicy() {
        return CalendarPolicy.SHIFT;
    }

    public SqlNode getCastSpec(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR:
                return new SqlDataTypeSpec(new SqlIdentifier("String", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case TINYINT:
                return new SqlDataTypeSpec(new SqlIdentifier("Int8", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case SMALLINT:
                return new SqlDataTypeSpec(new SqlIdentifier("Int16", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case INTEGER:
                return new SqlDataTypeSpec(new SqlIdentifier("Int32", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case BIGINT:
                return new SqlDataTypeSpec(new SqlIdentifier("Int64", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case FLOAT:
                return new SqlDataTypeSpec(new SqlIdentifier("Float32", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case DOUBLE:
                return new SqlDataTypeSpec(new SqlIdentifier("Float64", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case DATE:
                return new SqlDataTypeSpec(new SqlIdentifier("Date", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new SqlDataTypeSpec(new SqlIdentifier("DateTime", SqlParserPos.ZERO), type.getPrecision(), -1, (String) null, (TimeZone) null, SqlParserPos.ZERO);
            default:
                return super.getCastSpec(type);
        }
    }

    @Override
    public void unparseDateTimeLiteral(SqlWriter writer,
                                       SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
        String toFunc;
        if (literal instanceof SqlDateLiteral) {
            toFunc = "toDate";
        } else if (literal instanceof SqlTimestampLiteral) {
            toFunc = "toDateTime";
        } else if (literal instanceof SqlTimeLiteral) {
            toFunc = "toTime";
        } else {
            throw new RuntimeException("ClickHouse does not support DateTime literal: "
                    + literal);
        }

        writer.literal(toFunc + "('" + literal.toFormattedString() + "')");
    }

    @Override
    public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
                                   @Nullable SqlNode fetch) {
        requireNonNull(fetch, "fetch");

        writer.newlineAndIndent();
        final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.FETCH);
        writer.keyword("LIMIT");

        if (offset != null) {
            offset.unparse(writer, -1, -1);
            writer.sep(",", true);
        }

        fetch.unparse(writer, -1, -1);
        writer.endList(frame);
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call,
                            int leftPrec, int rightPrec) {
        if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
            RelToSqlConverterUtil.specialOperatorByName("substring")
                    .unparse(writer, call, 0, 0);
        } else {
            switch (call.getKind()) {
                case FLOOR:
                    if (call.operandCount() != 2) {
                        super.unparseCall(writer, call, leftPrec, rightPrec);
                        return;
                    }

                    unparseFloor(writer, call);
                    break;

                case COUNT:
                    // CH returns NULL rather than 0 for COUNT(DISTINCT) of NULL values.
                    // https://github.com/yandex/ClickHouse/issues/2494
                    // Wrap the call in a CH specific coalesce (assumeNotNull).
                    if (call.getFunctionQuantifier() != null
                            && call.getFunctionQuantifier().toString().equals("DISTINCT")) {
                        writer.print("assumeNotNull");
                        SqlWriter.Frame frame = writer.startList("(", ")");
                        super.unparseCall(writer, call, leftPrec, rightPrec);
                        writer.endList(frame);
                    } else {
                        super.unparseCall(writer, call, leftPrec, rightPrec);
                    }
                    break;

                default:
                    super.unparseCall(writer, call, leftPrec, rightPrec);
            }
        }
    }

    /**
     * Unparses datetime floor for ClickHouse.
     *
     * @param writer Writer
     * @param call   Call
     */
    private static void unparseFloor(SqlWriter writer, SqlCall call) {
        final SqlLiteral timeUnitNode = call.operand(1);
        TimeUnitRange unit = timeUnitNode.getValueAs(TimeUnitRange.class);

        String funName;
        switch (unit) {
            case YEAR:
                funName = "toStartOfYear";
                break;
            case MONTH:
                funName = "toStartOfMonth";
                break;
            case WEEK:
                funName = "toMonday";
                break;
            case DAY:
                funName = "toDate";
                break;
            case HOUR:
                funName = "toStartOfHour";
                break;
            case MINUTE:
                funName = "toStartOfMinute";
                break;
            default:
                throw new RuntimeException("ClickHouse does not support FLOOR for time unit: "
                        + unit);
        }

        writer.print(funName);
        SqlWriter.Frame frame = writer.startList("(", ")");
        call.operand(0).unparse(writer, 0, 0);
        writer.endList(frame);
    }
}