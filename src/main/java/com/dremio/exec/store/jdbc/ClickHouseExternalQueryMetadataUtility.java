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

package com.dremio.exec.store.jdbc;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataResponse;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.dialect.JdbcToFieldMapping;
import com.dremio.exec.store.jdbc.dialect.TypeMapper.AddPropertyCallback;
import com.dremio.exec.store.jdbc.dialect.TypeMapper.UnrecognizedTypeMarker;
import com.dremio.exec.tablefunctions.DremioCalciteResource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClickHouseExternalQueryMetadataUtility {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseExternalQueryMetadataUtility.class);
    private static final long METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS;

    private ClickHouseExternalQueryMetadataUtility() {
    }

    public static GetExternalQueryMetadataResponse getBatchSchema(DataSource source, JdbcDremioSqlDialect dialect, GetExternalQueryMetadataRequest request, JdbcPluginConfig config) {
        ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory(Thread.currentThread().getName() + ":jdbc-eq-metadata"));
        Callable<GetExternalQueryMetadataResponse> retrieveMetadata = () -> getExternalQueryMetadataFromSource(source, dialect, request, config);
        Future<GetExternalQueryMetadataResponse> future = executor.submit(retrieveMetadata);
        return handleMetadataFuture(future, executor, METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS);
    }

    static GetExternalQueryMetadataResponse handleMetadataFuture(Future<GetExternalQueryMetadataResponse> future, ExecutorService executor, long timeout) {
        GetExternalQueryMetadataResponse mdResponse;
        try {
            mdResponse = future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException response) {
            LOGGER.debug("Timeout while fetching metadata", response);
            throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(response));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(e));
        } catch (ExecutionException var13) {
            Throwable cause = var13.getCause();
            if (cause instanceof CalciteContextException) {
                throw (CalciteContextException)cause;
            }

            throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(cause));
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }

        return mdResponse;
    }

    private static void closeResource(Throwable throwable, AutoCloseable autoCloseable) throws Exception {
        if (throwable != null) {
            try {
                autoCloseable.close();
            } catch (Throwable throwable1) {
                throwable.addSuppressed(throwable1);
            }
        } else {
            autoCloseable.close();
        }

    }

    private static GetExternalQueryMetadataResponse getExternalQueryMetadataFromSource(DataSource source, JdbcDremioSqlDialect dialect, GetExternalQueryMetadataRequest request, JdbcPluginConfig config) throws Exception {
        Connection conn = source.getConnection();
        Throwable throwable1 = null;

        GetExternalQueryMetadataResponse response;
        try {
            PreparedStatement stmt = conn.prepareStatement(request.getSql());
            // Clickhouse won't provide metadata on a prepared statement without execution.
            // We are therefore executing the external statement select for 1 row.
            stmt.setMaxRows(1);
            stmt.executeQuery();
            LOGGER.debug("Prepared Statement: ",request.getSql());
            Throwable throwable2 = null;

            try {
                stmt.setQueryTimeout(Ints.saturatedCast(TimeUnit.MILLISECONDS.toSeconds(METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS)));
                ResultSetMetaData metaData = stmt.getMetaData();
                if (metaData == null) {
                    throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryInvalidError(config.getSourceName()));
                }

                List<JdbcToFieldMapping> mappings = dialect.getDataTypeMapper(config).mapJdbcToArrowFields((UnrecognizedTypeMarker)null, (AddPropertyCallback)null, (message) -> {
                    throw UserException.invalidMetadataError().addContext(message).buildSilently();
                }, conn, metaData, (Set)null, true);
                ByteString bytes = ByteString.copyFrom(BatchSchema.newBuilder().addFields((Iterable)mappings.stream().map(JdbcToFieldMapping::getField).collect(Collectors.toList())).build().serialize());
                response = GetExternalQueryMetadataResponse.newBuilder().setBatchSchema(bytes).build();
            } catch (Throwable throwable3) {
                throwable2 = throwable3;
                throw throwable3;
            } finally {
                if (stmt != null) {
                    closeResource(throwable2, stmt);
                }

            }
        } catch (Throwable throwable4) {
            throwable1 = throwable4;
            throw throwable4;
        } finally {
            if (conn != null) {
                closeResource(throwable1, conn);
            }

        }

        return response;
    }

    @VisibleForTesting
    static CalciteContextException newValidationError(ExInst<SqlValidatorException> exceptionExInst) {
        return SqlUtil.newContextException(SqlParserPos.ZERO, exceptionExInst);
    }

    static {
        METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS = TimeUnit.SECONDS.toMillis(120L);
    }
}
