package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

/**
 * @author Ryan Emerson
 */
public abstract class AbstractTableManager implements TableManager {

   private final Log log;
   protected final ConnectionFactory connectionFactory;
   protected final TableManipulationConfiguration config;

   protected String identifierQuoteString = "\"";
   protected String cacheName;
   protected DbMetaData metaData;
   protected TableName tableName;

   protected String insertRowSql;
   protected String updateRowSql;
   protected String upsertRowSql;
   protected String selectRowSql;
   protected String selectIdRowSql;
   protected String deleteRowSql;
   protected String loadAllRowsSql;
   protected String countRowsSql;
   protected String loadAllNonExpiredRowsSql;
   protected String deleteAllRows;
   protected String selectExpiredRowsSql;
   protected String deleteExpiredRowsSql;

   AbstractTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, Log log) {
      this.connectionFactory = connectionFactory;
      this.config = config;
      this.metaData = metaData;
      this.log = log;
   }

   @Override
   public void start() throws PersistenceException {
      if (config.createOnStart()) {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            if (!tableExists(conn)) {
               createTable(conn);
            }
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }
   }

   @Override
   public void stop() throws PersistenceException {
      if (config.dropOnExit()) {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            dropTable(conn);
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }
   }

   @Override
   public void setCacheName(String cacheName) {
      this.cacheName = cacheName;
      tableName = null;
   }

   public boolean tableExists(Connection connection) throws PersistenceException {
      return tableExists(connection, getTableName());
   }

   public boolean tableExists(Connection connection, TableName tableName) throws PersistenceException {
      Objects.requireNonNull(tableName, "table name is mandatory");
      ResultSet rs = null;
      try {
         // we need to make sure, that (even if the user has extended permissions) only the tables in current schema are checked
         // explicit set of the schema to the current user one to make sure only tables of the current users are requested
         DatabaseMetaData metaData = connection.getMetaData();
         String schemaPattern = tableName.getSchema();
         rs = metaData.getTables(null, schemaPattern, tableName.getName(), new String[]{"TABLE"});
         return rs.next();
      } catch (SQLException e) {
         if (log.isTraceEnabled())
            log.tracef(e, "SQLException occurs while checking the table %s", tableName);
         return false;
      } finally {
         JdbcUtil.safeClose(rs);
      }
   }

   public void createTable(Connection conn) throws PersistenceException {
      if (cacheName == null || cacheName.trim().length() == 0)
         throw new PersistenceException("cacheName needed in order to create table");

      List<String> idColumnNames = config.idColumnNames();
      List<String> idColumnTypes = config.idColumnTypes();
      List<String> dataColumnNames = config.dataColumnNames();
      List<String> dataColumnTypes = config.dataColumnTypes();
      StringBuilder buf = new StringBuilder();
      buf.append("CREATE TABLE ")
         .append(getTableName())
         .append(" (");
      for (int i = 0; i < idColumnNames.size(); i++) {
         buf.append(idColumnNames.get(i))
            .append(' ')
            .append(idColumnTypes.get(i))
            .append(" NOT NULL, ");
      }
      for (int i = 0; i < dataColumnNames.size(); i++) {
         // it's allowed to have the timestamp column as part of the value:
         if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
            buf.append(dataColumnNames.get(i))
               .append(' ')
               .append(dataColumnTypes.get(i))
               .append(", ");
         }
      }
      buf.append(config.timestampColumnName())
         .append(' ')
         .append(config.timestampColumnType())
         .append(", PRIMARY KEY (");
      for (int i = 0; i < idColumnNames.size(); i++) {
         if (i > 0) {
            buf.append(", ");
         }
         buf.append(idColumnNames.get(i));
      }
      buf.append("), KEY key_stamp (")
         .append(config.timestampColumnName())
         .append("))");
      String ddl = buf.toString();

      if (log.isTraceEnabled()) {
         log.tracef("Creating table with following DDL: '%s'.", ddl);
      }
      executeUpdateSql(conn, ddl);
   }

   public void executeUpdateSql(Connection conn, String sql) throws PersistenceException {
      Statement statement = null;
      try {
         statement = conn.createStatement();
         statement.executeUpdate(sql);
      } catch (SQLException e) {
         log.errorCreatingTable(sql, e);
         throw new PersistenceException(e);
      } finally {
         JdbcUtil.safeClose(statement);
      }
   }

   public void dropTable(Connection conn) throws PersistenceException {
      String dropTableDdl = "DROP TABLE " + getTableName();
      String clearTable = "DELETE FROM " + getTableName();
      executeUpdateSql(conn, clearTable);
      if (log.isTraceEnabled()) {
         log.tracef("Dropping table with following DDL '%s'", dropTableDdl);
      }
      executeUpdateSql(conn, dropTableDdl);
   }

   public int getFetchSize() {
      return config.fetchSize();
   }

   public int getBatchSize() {
      return config.batchSize();
   }

   @Override
   public boolean isUpsertSupported() {
      return !metaData.isUpsertDisabled();
   }

   public String getIdentifierQuoteString() {
      return identifierQuoteString;
   }

   public TableName getTableName() {
      if (tableName == null) {
         tableName = new TableName(identifierQuoteString, config.tableNamePrefix(), cacheName);
      }
      return tableName;
   }

   @Override
   public String getInsertRowSql() {
      if (insertRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("INSERT INTO ")
            .append(getTableName())
            .append(" (");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append(dataColumnNames.get(i))
                  .append(", ");
            }
         }
         buf.append(config.timestampColumnName());
         for (int i = 0; i < idColumnNames.size(); i++) {
            buf.append(", ")
               .append(idColumnNames.get(i));
         }
         buf.append(") VALUES (");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append("?, ");
            }
         }
         for (int i = 0; i < idColumnNames.size(); i++) {
            buf.append("?, ");
         }
         buf.append("?)");
         insertRowSql = buf.toString();
      }
      return insertRowSql;
   }

   @Override
   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("UPDATE ")
            .append(getTableName())
            .append(" SET ");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append(dataColumnNames.get(i))
                  .append(" = ?, ");
            }
         }
         buf.append(config.timestampColumnName())
            .append(" = ? WHERE ");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(" AND ");
            }
            buf.append(idColumnNames.get(i))
               .append(" = ?");
         }
         updateRowSql = buf.toString();
      }
      return updateRowSql;
   }

   @Override
   public String getSelectRowSql() {
      if (selectRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("SELECT ");
         for (int i = 0; i < idColumnNames.size(); i++) {
            buf.append(idColumnNames.get(i))
               .append(", ");
         }
         for (int i = 0; i < dataColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(", ");
            }
            buf.append(dataColumnNames.get(i));
         }
         buf.append(" FROM ")
            .append(getTableName())
            .append(" WHERE ");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(" AND ");
            }
            buf.append(idColumnNames.get(i))
               .append(" = ?");
         }
         selectRowSql = buf.toString();
      }
      return selectRowSql;
   }

   @Override
   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("SELECT ");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(", ");
            }
            buf.append(idColumnNames.get(i));
         }
         buf.append(" FROM ")
            .append(getTableName())
            .append(" WHERE ");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(" AND ");
            }
            buf.append(idColumnNames.get(i))
               .append(" = ?");
         }
         selectIdRowSql = buf.toString();
      }
      return selectIdRowSql;
   }

   @Override
   public String getCountRowsSql() {
      if (countRowsSql == null) {
         countRowsSql = "SELECT COUNT(*) FROM " + getTableName();
      }
      return countRowsSql;
   }

   @Override
   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("DELETE FROM ")
            .append(getTableName())
            .append(" WHERE ");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(" AND ");
            }
            buf.append(idColumnNames.get(i))
               .append(" = ?");
         }
         deleteRowSql = buf.toString();
      }
      return deleteRowSql;
   }

   @Override
   public String getLoadNonExpiredAllRowsSql() {
      if (loadAllNonExpiredRowsSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("SELECT ");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            buf.append(dataColumnNames.get(i))
               .append(", ");
         }
         for (int i = 0; i < idColumnNames.size(); i++) {
            buf.append(idColumnNames.get(i))
               .append(", ");
         }
         buf.append(config.timestampColumnName())
            .append(" FROM ")
            .append(getTableName())
            .append(" WHERE ")
            .append(config.timestampColumnName())
            .append(" > ? OR ")
            .append(config.timestampColumnName())
            .append(" < 0");
         loadAllNonExpiredRowsSql = buf.toString();
      }
      return loadAllNonExpiredRowsSql;
   }

   @Override
   public String getLoadAllRowsSql() {
      if (loadAllRowsSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("SELECT ");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            buf.append(dataColumnNames.get(i))
               .append(", ");
         }
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(", ");
            }
            buf.append(idColumnNames.get(i));
         }
         buf.append(" FROM ")
            .append(getTableName());
         loadAllRowsSql = buf.toString();
      }
      return loadAllRowsSql;
   }

   @Override
   public String getDeleteAllRowsSql() {
      if (deleteAllRows == null) {
         deleteAllRows = "DELETE FROM " + getTableName();
      }
      return deleteAllRows;
   }

   @Override
   public String getSelectExpiredRowsSql() {
      if (selectExpiredRowsSql == null) {
         selectExpiredRowsSql = String.format("%s WHERE %s < ?", getLoadAllRowsSql(), config.timestampColumnName());
      }
      return selectExpiredRowsSql;
   }

   @Override
   public String getDeleteExpiredRowsSql() {
      if (deleteExpiredRowsSql == null) {
         deleteExpiredRowsSql = String.format("DELETE FROM %1$s WHERE %2$s < ? AND %2$s > 0", getTableName(), config.timestampColumnName());
      }
      return deleteExpiredRowsSql;
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("MERGE INTO ")
            .append(getTableName())
            .append(" USING (VALUES (");
         for (int i = 0; i < dataColumnNames.size() + idColumnNames.size(); i++) {
            buf.append("?, ");
         }
         buf.append("?)) AS tmp (");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append(dataColumnNames.get(i))
                  .append(", ");
            }
         }
         buf.append(config.timestampColumnName());
         for (int i = 0; i < idColumnNames.size(); i++) {
            buf.append(", ")
               .append(idColumnNames.get(i));
         }
         buf.append("ON (");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(" AND ");
            }
            buf.append(idColumnNames.get(i))
               .append(" = tmp.")
               .append(idColumnNames.get(i));
         }
         buf.append(") WHEN MATCHED THEN UPDATE SET ");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append(dataColumnNames.get(i))
                  .append(" = tmp.")
                  .append(dataColumnNames.get(i))
                  .append(", ");
            }
         }
         buf.append(config.timestampColumnName())
            .append(" = tmp.")
            .append(config.timestampColumnName())
            .append(" WHEN NOT MATCHED THEN INSERT (");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append(dataColumnNames.get(i))
                  .append(", ");
            }
         }
         buf.append(config.timestampColumnName())
            .append(") VALUES (");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append("tmp.")
                  .append(dataColumnNames.get(i))
                  .append(", ");
            }
         }
         buf.append("tmp.")
            .append(config.timestampColumnName())
            .append(')');
         upsertRowSql = buf.toString();

      }
      return upsertRowSql;
   }
}
