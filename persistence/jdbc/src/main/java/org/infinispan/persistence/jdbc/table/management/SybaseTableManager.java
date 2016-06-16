package org.infinispan.persistence.jdbc.table.management;

import java.util.List;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class SybaseTableManager extends AbstractTableManager {
   private static final Log LOG = LogFactory.getLog(SybaseTableManager.class, Log.class);

   SybaseTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> idColumnTypes = config.idColumnTypes();
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
               .append(" = convert(")
               .append(idColumnTypes.get(i))
               .append(", ?)");
         }
         updateRowSql = buf.toString();
      }
      return updateRowSql;
   }

   @Override
   public String getSelectRowSql() {
      if (selectRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> idColumnTypes = config.idColumnTypes();
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
               .append(" = convert(")
               .append(idColumnTypes.get(i))
               .append(", ?)");
         }
         selectRowSql = buf.toString();
      }
      return selectRowSql;
   }

   @Override
   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> idColumnTypes = config.idColumnTypes();
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
               .append(" = convert(")
               .append(idColumnTypes.get(i))
               .append(", ?)");
         }
         selectIdRowSql = buf.toString();
      }
      return selectIdRowSql;
   }

   @Override
   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> idColumnTypes = config.idColumnTypes();
         StringBuilder buf = new StringBuilder();
         buf.append("DELETE FROM ")
            .append(getTableName())
            .append(" WHERE ");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(" AND ");
            }
            buf.append(idColumnNames.get(i))
               .append(" = convert(")
               .append(idColumnTypes.get(i))
               .append(", ?)");
         }
         deleteRowSql = buf.toString();
      }
      return deleteRowSql;
   }
}