package org.infinispan.persistence.jdbc.table.management;

import java.util.List;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class H2TableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(H2TableManager.class, Log.class);

   H2TableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         List<String> idColumnNames = config.idColumnNames();
         List<String> dataColumnNames = config.dataColumnNames();
         StringBuilder buf = new StringBuilder();
         buf.append("MERGE INTO ")
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
         buf.append(") KEY(");
         for (int i = 0; i < idColumnNames.size(); i++) {
            if (i > 0) {
               buf.append(", ");
            }
            buf.append(idColumnNames.get(i));
         }
         buf.append(") VALUES(");
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
         upsertRowSql = buf.toString();
      }
      return upsertRowSql;
   }
}
