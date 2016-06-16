package org.infinispan.persistence.jdbc.table.management;

import java.util.List;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class MySQLTableManager extends AbstractTableManager {
   private static final Log LOG = LogFactory.getLog(MySQLTableManager.class, Log.class);

   MySQLTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
      identifierQuoteString = "`";
   }

   @Override
   public int getFetchSize() {
      return Integer.MIN_VALUE;
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         // Assumes that config.idColumnName is the primary key
         buf.append(getInsertRowSql())
            .append(" ON DUPLICATE KEY UPDATE ");
         for (int i = 0; i < dataColumnNames.size(); i++) {
            // it's allowed to have the timestamp column as part of the value:
            if (!dataColumnNames.get(i).equals(config.timestampColumnName())) {
               buf.append(dataColumnNames.get(i))
                  .append(" = VALUES(")
                  .append(dataColumnNames.get(i))
                  .append("), ");
            }
         }
         buf.append(config.timestampColumnName())
            .append(" = VALUES(")
            .append(config.timestampColumnName())
            .append(')');
         upsertRowSql = buf.toString();
      }
      return upsertRowSql;
   }
}
