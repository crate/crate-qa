package stock_jdbc;

import io.crate.testing.CrateTestCluster;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class JdbcMetaDataTest {

    @ClassRule
    public static final CrateTestCluster TEST_CLUSTER = CrateTestCluster
        .fromURL("https://cdn.crate.io/downloads/releases/nightly/crate-latest.tar.gz")
        .settings(Map.of("psql.port", 55432))
        .build();
    public static final String URL = "jdbc:postgresql://localhost:55432/doc?user=crate";

    @Test
    public void test_allProceduresAreCallable() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().allProceduresAreCallable()).isTrue();
        }
    }

    @Test
    public void test_allTablesAreSelectable() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().allTablesAreSelectable()).isTrue();
        }
    }

    @Test
    public void test_autoCommitFailureClosesAllResultSets() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().autoCommitFailureClosesAllResultSets()).isFalse();
        }
    }

    @Test
    public void test_dataDefinitionCausesTransactionCommit_TODO() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().dataDefinitionCausesTransactionCommit()).isFalse();
        }
    }

    @Test
    public void test_dataDefinitionIgnoredInTransactions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().dataDefinitionIgnoredInTransactions()).isFalse();
        }
    }

    @Test
    public void test_deletesAreDetected() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        }
    }

    @Test
    public void test_doesMaxRowSizeIncludeBlobs() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().doesMaxRowSizeIncludeBlobs()).isFalse();
        }
    }

    @Test
    public void test_generatedKeyAlwaysReturned() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().generatedKeyAlwaysReturned()).isTrue();
        }
    }

    @Test
    @Ignore("Not implemented in PostgreSQL JDBC")
    public void test_getAttributes() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            conn.getMetaData().getAttributes(null, null, null, null);
        }
    }

    @Test
    public void test_getBestRowIdentifier() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var result = conn.getMetaData().getBestRowIdentifier(null, "sys", "summits", DatabaseMetaData.bestRowSession, true);
            assertThat(result.next()).isTrue();
        }
    }

    @Test
    public void test_getCatalogSeparator() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getCatalogSeparator()).isEqualTo(".");
        }
    }

    @Test
    public void test_getCatalogTerm() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getCatalogTerm()).isEqualTo("database");
        }
    }

    @Test
    public void test_getCatalogs() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var result = conn.getMetaData().getCatalogs();
            assertThat(result.next()).isTrue();
            // Returns `crate` as of pgjdbc 42.7.0. It returned `doc` before.
            assertThat(result.getString(1)).isEqualTo("crate");
        }
    }

    @Test
    public void test_getClientInfoProperties() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var result = conn.getMetaData().getClientInfoProperties();
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("ApplicationName");
        }
    }

    @Test
    @Ignore("https://github.com/crate/crate/issues/9568")
    public void test_getColumnPrivileges() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getColumnPrivileges(null, "sys", "summits", null);
            assertThat(results.next()).isTrue();
        }
    }

    @Test
    public void test_getColumns() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getColumns(null, "sys", "summits", null);
            assertThat(results.next()).isTrue();
            assertThat(results.getString(3)).isEqualTo("summits");
            assertThat(results.getString(4)).isEqualTo("classification");
        }
    }

    @Test
    public void test_getCrossReference() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getCrossReference(null, "sys", "jobs", null, "sys", "jobs_log");
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    public void test_getDatabaseMajorVersion() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getDatabaseMajorVersion()).isEqualTo(14);
        }
    }

    @Test
    public void test_getDatabaseMinorVersion() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getDatabaseMinorVersion()).isZero();
        }
    }

    @Test
    public void test_getDatabaseProductName() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getDatabaseProductName()).isEqualTo("PostgreSQL");
        }
    }

    @Test
    public void test_getDatabaseProductVersion() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getDatabaseProductVersion()).isEqualTo("14.0");
        }
    }

    @Test
    @Ignore("Not supported by CrateDB after pgjdbc 42.7.0 changed the implementation")
    // https://github.com/crate/crate/issues/15113
    public void test_getDefaultTransactionIsolation() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getDefaultTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    @Test
    public void test_getExportedKeys() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getExportedKeys(null, "sys", "summits");
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    public void test_getExtraNameCharacters() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getExtraNameCharacters()).isEqualTo("");
        }
    }

    @Test
    public void test_getFunctionColumns() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getFunctionColumns(null, null, "substr", null);
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    public void test_getFunctions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getFunctions(null, null, "current_schema");
            assertThat(results.next()).isTrue();
        }
    }

    @Test
    public void test_getIdentifierQuoteString() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getIdentifierQuoteString()).isEqualTo("\"");
        }
    }

    @Test
    public void test_getImportedKeys() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getImportedKeys(null, "sys", "summits");
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    @Ignore("Blocked by https://github.com/crate/crate/issues/17049")
    // Error: Unknown function: pg_catalog.pg_get_indexdef(tmp.ci_oid, tmp.ordinal_position, false)")
    public void test_getIndexInfo() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getIndexInfo(null, "sys", "summits", true, true);
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    public void test_getMaxBinaryLiteralLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxBinaryLiteralLength()).isZero();
        }
    }

    @Test
    public void test_getMaxCatalogNameLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxCatalogNameLength()).isEqualTo(63);
        }
    }

    @Test
    public void test_getMaxCharLiteralLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxCharLiteralLength()).isZero();
        }
    }

    @Test
    public void test_getMaxColumnNameLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxColumnNameLength()).isEqualTo(63);
        }
    }

    @Test
    public void test_getMaxColumnsInGroupBy() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxColumnsInGroupBy()).isZero();
        }
    }

    @Test
    public void test_getMaxColumnsInIndex() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxColumnsInIndex()).isEqualTo(32);
        }
    }

    @Test
    public void test_getMaxColumnsInOrderBy() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxColumnsInOrderBy()).isZero();
        }
    }

    @Test
    public void test_getMaxColumnsInSelect() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxColumnsInSelect()).isZero();
        }
    }

    @Test
    public void test_getMaxColumnsInTable() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxColumnsInTable()).isEqualTo(1600);
        }
    }

    @Test
    public void test_getMaxConnections() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxConnections()).isEqualTo(8192);
        }
    }

    @Test
    public void test_getMaxCursorNameLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxCursorNameLength()).isEqualTo(63);
        }
    }

    @Test
    public void test_getMaxIndexLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxIndexLength()).isZero();
        }
    }

    @Test
    public void test_getMaxLogicalLobSize() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxLogicalLobSize()).isEqualTo(0L);
        }
    }

    @Test
    public void test_getMaxProcedureNameLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxProcedureNameLength()).isEqualTo(63);
        }
    }

    @Test
    public void test_getMaxRowSize() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxRowSize()).isEqualTo(1073741824);
        }
    }

    @Test
    public void test_getMaxSchemaNameLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxSchemaNameLength()).isEqualTo(63);
        }
    }

    @Test
    public void test_getMaxStatementLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxStatementLength()).isZero();
        }
    }

    @Test
    public void test_getMaxStatements() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxStatements()).isZero();
        }
    }

    @Test
    public void test_getMaxTableNameLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxTableNameLength()).isEqualTo(63);
        }
    }

    @Test
    public void test_getMaxTablesInSelect() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxTablesInSelect()).isZero();
        }
    }

    @Test
    public void test_getMaxUserNameLength() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getMaxUserNameLength()).isEqualTo(63);
        }
    }

    @Test
    public void tes_getNumericFunctions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getNumericFunctions()).isEqualTo(
                "abs,acos,asin,atan,atan2,ceiling,cos,cot,degrees,exp,floor,log,log10,mod,pi,power,radians,round,sign,sin,sqrt,tan,truncate");
        }
    }

    @Test
    public void test_getPrimaryKeys() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getPrimaryKeys(null, null, null);
            assertThat(results.next()).isTrue();
        }
    }

    @Test
    public void test_getProcedureColumns() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getProcedureColumns(null, null, null, null);
            assertThat(results.next()).isTrue();
        }
    }

    @Test
    public void test_getProcedureTerm() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getProcedureTerm()).isEqualTo("function");
        }
    }

    @Test
    public void test_getProcedures() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getProcedures(null, null, null);
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    @Ignore("Not implemented by PostgreSQL JDBC")
    public void test_getPseudoColumns() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            conn.getMetaData().getPseudoColumns(null, "sys", "summits", "m");
        }
    }

    @Test
    public void test_getResultSetHoldability() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getResultSetHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        }
    }

    @Test
    @Ignore("Not implemented by PostgreSQL JDBC")
    public void test_getRowIdLifetime() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getRowIdLifetime()).isEqualTo(RowIdLifetime.ROWID_UNSUPPORTED);
        }
    }

    @Test
    public void test_getSQLKeywords() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getSQLKeywords()).contains("summary");
        }
    }

    @Test
    public void test_getSQLStateType() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getSQLStateType()).isEqualTo(DatabaseMetaData.sqlStateSQL);
        }
    }

    @Test
    public void test_getSchemaTerm() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getSchemaTerm()).isEqualTo("schema");
        }
    }

    @Test
    public void test_getSchemas() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getSchemas();
            assertThat(results.next()).isTrue();
            assertThat(results.getString(1)).isEqualTo("blob");
        }
    }

    @Test
    public void test_getSearchStringEscape() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getSearchStringEscape()).isEqualTo("\\");
        }
    }

    @Test
    public void test_getStringFunctions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getStringFunctions()).isEqualTo(
                "ascii,char,concat,lcase,left,length,ltrim,repeat,rtrim,space,substring,ucase,replace");
        }
    }

    @Test
    @Ignore("Not implemented in PostgreSQL JDBC")
    public void test_getSuperTables() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            conn.getMetaData().getSuperTables(null, "sys", "summits");
        }
    }

    @Test
    @Ignore("Not implemented in PostgreSQL JDBC")
    public void test_getSuperTypes() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            conn.getMetaData().getSuperTypes(null, "sys", "t");
        }
    }

    @Test
    public void test_getSystemFunctions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getSystemFunctions()).isEqualTo("database,ifnull,user");
        }
    }

    @Test
    public void test_getTablePrivileges() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getTablePrivileges(null, "sys", "summits");
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    public void test_getTableTypes() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getTableTypes();
            assertThat(results.next()).isTrue();
        }
    }

    @Test
    public void test_getTables() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getTables(null, "sys", null, null);
            assertThat(results.next()).isTrue();
            assertThat(results.getString(3)).isEqualTo("allocations_pkey");
        }
    }

    @Test
    public void test_getTimeDateFunctions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().getTimeDateFunctions()).isEqualTo(
                "curdate,curtime,dayname,dayofmonth,dayofweek,dayofyear,hour,minute,month,monthname,now,quarter,second,week,year,timestampadd");
        }
    }

    @Test
    public void test_getTypeInfo() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getTypeInfo();
            assertThat(results.next()).isTrue();
        }
    }

    @Test
    public void test_getUDTs() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var results = conn.getMetaData().getUDTs(null, "sys", "t", new int[0]);
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    public void test_getVersionColumns() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
             var results = conn.getMetaData().getVersionColumns(null, "sys", "summits");
             assertThat(results.next()).isTrue();
             assertThat(results.getString(2)).isEqualTo("ctid");
        }
    }

    @Test
    public void test_insertsAreDetected() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        }
    }

    @Test
    public void test_isCatalogAtStart() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().isCatalogAtStart()).isTrue();
        }
    }

    @Test
    public void test_locatorsUpdateCopy() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().locatorsUpdateCopy()).isTrue();
        }
    }

    @Test
    public void test_nullPlusNonNullIsNull() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().nullPlusNonNullIsNull()).isTrue();
        }
    }

    @Test
    public void test_nullsAreSortedAtEnd() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().nullsAreSortedAtEnd()).isFalse();
        }
    }

    @Test
    public void test_nullsAreSortedAtStart() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().nullsAreSortedAtStart()).isFalse();
        }
    }

    @Test
    public void test_nullsAreSortedHigh() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().nullsAreSortedHigh()).isTrue();
        }
    }

    @Test
    public void test_nullsAreSortedLow() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().nullsAreSortedLow()).isFalse();
        }
    }

    @Test
    public void test_othersDeletesAreVisible() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        }
    }

    @Test
    public void test_othersInsertsAreVisible() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        }
    }

    @Test
    public void test_othersUpdatesAreVisible() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        }
    }

    @Test
    public void test_ownDeletesAreVisible() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        }
    }

    @Test
    public void test_ownInsertsAreVisible() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        }
    }

    @Test
    public void test_ownUpdatesAreVisible() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        }
    }

    @Test
    public void test_storesLowerCaseIdentifiers() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().storesLowerCaseIdentifiers()).isTrue();
        }
    }

    @Test
    public void test_storesLowerCaseQuotedIdentifiers() throws Exception  {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().storesLowerCaseQuotedIdentifiers()).isFalse();
        }
    }

    @Test
    public void test_storesMixedCaseIdentifiers() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().storesMixedCaseIdentifiers()).isFalse();
        }
    }

    @Test
    public void test_storesMixedCaseQuotedIdentifiers() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().storesMixedCaseQuotedIdentifiers()).isFalse();
        }
    }

    @Test
    public void test_storesUpperCaseIdentifiers() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().storesUpperCaseIdentifiers()).isFalse();
        }
    }

    @Test
    public void test_storesUpperCaseQuotedIdentifiers() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().storesUpperCaseQuotedIdentifiers()).isFalse();
        }
    }

    @Test
    public void test_supportsANSI92EntryLevelSQL() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsANSI92EntryLevelSQL()).isTrue();
        }
    }

    @Test
    public void test_supportsANSI92FullSQL() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsANSI92FullSQL()).isFalse();
        }
    }

    @Test
    public void test_supportsANSI92IntermediateSQL() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsANSI92IntermediateSQL()).isFalse();
        }
    }

    @Test
    public void test_supportsAlterTableWithAddColumn() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsAlterTableWithAddColumn()).isTrue();
        }
    }

    @Test
    public void test_supportsAlterTableWithDropColumn() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsAlterTableWithDropColumn()).isTrue();
        }
    }

    @Test
    public void test_supportsBatchUpdates() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsBatchUpdates()).isTrue();
        }
    }

    @Test
    public void test_supportsCatalogsInDataManipulation() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsCatalogsInDataManipulation()).isFalse();
        }
    }

    @Test
    public void test_supportsCatalogsInIndexDefinitions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsCatalogsInIndexDefinitions()).isFalse();
        }
    }

    @Test
    public void test_supportsCatalogsInPrivilegeDefinitions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsCatalogsInPrivilegeDefinitions()).isFalse();
        }
    }

    @Test
    public void test_supportsCatalogsInProcedureCalls() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsCatalogsInProcedureCalls()).isFalse();
        }
    }

    @Test
    public void test_supportsCatalogsInTableDefinitions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsCatalogsInTableDefinitions()).isFalse();
        }
    }

    @Test
    public void test_supportsColumnAliasing() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsColumnAliasing()).isTrue();
        }
    }

    @Test
    public void test_supportsConvert() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsConvert()).isFalse();
        }
    }

    @Test
    public void test_supportsConvertWithArgs() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsConvert(1, 1)).isFalse();
        }
    }

    @Test
    public void test_supportsCoreSQLGrammar() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsCoreSQLGrammar()).isFalse();
        }
    }

    @Test
    public void test_supportsCorrelatedSubqueries() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsCorrelatedSubqueries()).isTrue();
        }
    }

    @Test
    public void test_supportsDataDefinitionAndDataManipulationTransactions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()).isTrue();
        }
    }

    @Test
    public void test_supportsDataManipulationTransactionsOnly() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsDataManipulationTransactionsOnly()).isFalse();
        }
    }

    @Test
    public void test_supportsDifferentTableCorrelationNames() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsDifferentTableCorrelationNames()).isFalse();
        }
    }

    @Test
    public void test_supportsExpressionsInOrderBy() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsExpressionsInOrderBy()).isTrue();
        }
    }

    @Test
    public void test_supportsExtendedSQLGrammar() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsExtendedSQLGrammar()).isFalse();
        }
    }

    @Test
    public void test_supportsFullOuterJoins() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsFullOuterJoins()).isTrue();
        }
    }

    @Test
    public void test_supportsGetGeneratedKeys() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsGetGeneratedKeys()).isTrue();
        }
    }

    @Test
    public void test_supportsGroupBy() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsGroupBy()).isTrue();
        }
    }

    @Test
    public void test_supportsGroupByBeyondSelect() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsGroupByBeyondSelect()).isTrue();
        }
    }

    @Test
    public void test_supportsGroupByUnrelated() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsGroupByUnrelated()).isTrue();
        }
    }

    @Test
    public void test_supportsIntegrityEnhancementFacility() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsIntegrityEnhancementFacility()).isTrue();
        }
    }

    @Test
    public void test_supportsLikeEscapeClause() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsLikeEscapeClause()).isTrue();
        }
    }

    @Test
    public void test_supportsLimitedOuterJoins() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsLimitedOuterJoins()).isTrue();
        }
    }

    @Test
    public void test_supportsMinimumSQLGrammar() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsMinimumSQLGrammar()).isTrue();
        }
    }

    @Test
    public void test_supportsMixedCaseIdentifiers() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsMixedCaseIdentifiers()).isFalse();
        }
    }

    @Test
    public void test_supportsMixedCaseQuotedIdentifiers() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsMixedCaseQuotedIdentifiers()).isTrue();
        }
    }

    @Test
    public void test_supportsMultipleOpenResults() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsMultipleOpenResults()).isFalse();
        }
    }

    @Test
    public void test_supportsMultipleResultSets() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsMultipleResultSets()).isTrue();
        }
    }

    @Test
    public void test_supportsMultipleTransactions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsMultipleTransactions()).isTrue();
        }
    }

    @Test
    public void test_supportsNamedParameters() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsNamedParameters()).isFalse();
        }
    }

    @Test
    public void test_supportsNonNullableColumns() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsNonNullableColumns()).isTrue();
        }
    }

    @Test
    public void test_supportsOpenCursorsAcrossCommit() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsOpenCursorsAcrossCommit()).isFalse();
        }
    }

    @Test
    public void test_supportsOpenCursorsAcrossRollback() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsOpenCursorsAcrossRollback()).isFalse();
        }
    }

    @Test
    public void test_supportsOpenStatementsAcrossCommit() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsOpenStatementsAcrossCommit()).isTrue();
        }
    }

    @Test
    public void test_supportsOpenStatementsAcrossRollback() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsOpenStatementsAcrossRollback()).isTrue();
        }
    }

    @Test
    public void test_supportsOrderByUnrelated() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsOrderByUnrelated()).isTrue();
        }
    }

    @Test
    public void test_supportsOuterJoins() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsOuterJoins()).isTrue();
        }
    }

    @Test
    public void test_supportsPositionedDelete() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsPositionedDelete()).isFalse();
        }
    }

    @Test
    public void test_supportsPositionedUpdate() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsPositionedUpdate()).isFalse();
        }
    }

    @Test
    public void test_supportsRefCursors() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsRefCursors()).isTrue();
        }
    }

    @Test
    public void test_supportsResultSetConcurrency() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(
                conn.getMetaData().supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .isTrue();
        }
    }

    @Test
    public void test_supportsResultSetHoldability() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT)).isTrue();
        }
    }

    @Test
    public void test_supportsResultSetType() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
        }
    }

    @Test
    public void test_supportsSavepoints() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSavepoints()).isTrue();
        }
    }

    @Test
    public void test_supportsSchemasInDataManipulation() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSchemasInDataManipulation()).isTrue();
        }
    }

    @Test
    public void test_supportsSchemasInIndexDefinitions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSchemasInIndexDefinitions()).isTrue();
        }
    }

    @Test
    public void test_supportsSchemasInPrivilegeDefinitions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSchemasInPrivilegeDefinitions()).isTrue();
        }
    }

    @Test
    public void test_supportsSchemasInProcedureCalls() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSchemasInProcedureCalls()).isTrue();
        }
    }

    @Test
    public void test_supportsSchemasInTableDefinitions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSchemasInTableDefinitions()).isTrue();
        }
    }

    @Test
    public void test_supportsSelectForUpdate() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSelectForUpdate()).isTrue();
        }
    }

    @Test
    public void test_supportsSharding() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSharding()).isFalse();
        }
    }

    @Test
    public void test_supportsStatementPooling() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsStatementPooling()).isFalse();
        }
    }

    @Test
    public void test_supportsStoredFunctionsUsingCallSyntax() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsStoredFunctionsUsingCallSyntax()).isTrue();
        }
    }

    @Test
    public void test_supportsStoredProcedures() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsStoredProcedures()).isTrue();
        }
    }

    @Test
    public void test_supportsSubqueriesInComparisons() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSubqueriesInComparisons()).isTrue();
        }
    }

    @Test
    public void test_supportsSubqueriesInExists() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSubqueriesInExists()).isTrue();
        }
    }

    @Test
    public void test_supportsSubqueriesInIns() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSubqueriesInIns()).isTrue();
        }
    }

    @Test
    public void test_supportsSubqueriesInQuantifieds() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsSubqueriesInQuantifieds()).isTrue();
        }
    }

    @Test
    public void test_supportsTableCorrelationNames() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsTableCorrelationNames()).isTrue();
        }
    }

    @Test
    public void test_supportsTransactionIsolationLevel() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED)).isTrue();
        }
    }

    @Test
    public void test_supportsTransactions() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsTransactions()).isTrue();
        }
    }

    @Test
    public void test_supportsUnion() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsUnion()).isTrue();
        }
    }

    @Test
    public void test_supportsUnionAll() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().supportsUnionAll()).isTrue();
        }
    }

    @Test
    public void test_updatesAreDetected() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
        }
    }

    @Test
    public void test_usesLocalFilePerTable() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().usesLocalFilePerTable()).isFalse();
        }
    }

    @Test
    public void test_usesLocalFiles() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            assertThat(conn.getMetaData().usesLocalFiles()).isFalse();
        }
    }
}
