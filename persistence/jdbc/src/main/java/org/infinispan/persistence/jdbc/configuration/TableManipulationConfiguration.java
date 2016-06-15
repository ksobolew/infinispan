package org.infinispan.persistence.jdbc.configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.table.management.TableManager;

public class TableManipulationConfiguration {
   public static final AttributeDefinition<List<String>> ID_COLUMN_NAMES = AttributeDefinition.builder("idColumnName", Collections.<String> emptyList()).copier(v -> new ArrayList<>(v)).immutable().build();
   public static final AttributeDefinition<List<String>> ID_COLUMN_TYPES = AttributeDefinition.builder("idColumnType", Collections.<String> emptyList()).copier(v -> new ArrayList<>(v)).immutable().build();
   public static final AttributeDefinition<String> TABLE_NAME_PREFIX = AttributeDefinition.builder("tableNamePrefix", null, String.class).immutable().build();
   public static final AttributeDefinition<String> CACHE_NAME = AttributeDefinition.builder("cacheName", null, String.class).immutable().build();
   public static final AttributeDefinition<List<String>> DATA_COLUMN_NAMES = AttributeDefinition.builder("dataColumnName", Collections.<String> emptyList()).copier(v -> new ArrayList<>(v)).immutable().build();
   public static final AttributeDefinition<List<String>> DATA_COLUMN_TYPES = AttributeDefinition.builder("dataColumnType", Collections.<String> emptyList()).copier(v -> new ArrayList<>(v)).immutable().build();
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_NAME = AttributeDefinition.builder("timestampColumnName", null, String.class).immutable().build();
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_TYPE = AttributeDefinition.builder("timestampColumnType", null, String.class).immutable().build();
   public static final AttributeDefinition<Integer> BATCH_SIZE = AttributeDefinition.builder("batchSize", TableManager.DEFAULT_BATCH_SIZE).immutable().build();
   public static final AttributeDefinition<Integer> FETCH_SIZE = AttributeDefinition.builder("fetchSize", TableManager.DEFAULT_FETCH_SIZE).immutable().build();
   public static final AttributeDefinition<Boolean> CREATE_ON_START = AttributeDefinition.builder("createOnStart", true).immutable().build();
   public static final AttributeDefinition<Boolean> DROP_ON_EXIT = AttributeDefinition.builder("dropOnExit", false).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TableManipulationConfiguration.class, ID_COLUMN_NAMES, ID_COLUMN_TYPES, TABLE_NAME_PREFIX, CACHE_NAME, DATA_COLUMN_NAMES, DATA_COLUMN_TYPES,
                              TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_TYPE, BATCH_SIZE, FETCH_SIZE, CREATE_ON_START, DROP_ON_EXIT);
   }

   private final Attribute<List<String>> idColumnNames;
   private final Attribute<List<String>> idColumnTypes;
   private final Attribute<String> tableNamePrefix;
   private final Attribute<String> cacheName;
   private final Attribute<List<String>> dataColumnNames;
   private final Attribute<List<String>> dataColumnTypes;
   private final Attribute<String> timestampColumnName;
   private final Attribute<String> timestampColumnType;
   private final Attribute<Integer> batchSize;
   private final Attribute<Integer> fetchSize;
   private final Attribute<Boolean> createOnStart;
   private final Attribute<Boolean> dropOnExit;
   private final AttributeSet attributes;

   TableManipulationConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      idColumnNames = attributes.attribute(ID_COLUMN_NAMES);
      idColumnTypes = attributes.attribute(ID_COLUMN_TYPES);
      tableNamePrefix = attributes.attribute(TABLE_NAME_PREFIX);
      cacheName = attributes.attribute(CACHE_NAME);
      dataColumnNames = attributes.attribute(DATA_COLUMN_NAMES);
      dataColumnTypes = attributes.attribute(DATA_COLUMN_TYPES);
      timestampColumnName = attributes.attribute(TIMESTAMP_COLUMN_NAME);
      timestampColumnType = attributes.attribute(TIMESTAMP_COLUMN_TYPE);
      batchSize = attributes.attribute(BATCH_SIZE);
      fetchSize = attributes.attribute(FETCH_SIZE);
      createOnStart = attributes.attribute(CREATE_ON_START);
      dropOnExit = attributes.attribute(DROP_ON_EXIT);
   }

   public boolean createOnStart() {
      return createOnStart.get();
   }

   public boolean dropOnExit() {
      return dropOnExit.get();
   }

   public List<String> idColumnNames() {
      return Collections.unmodifiableList(idColumnNames.get());
   }

   public List<String> idColumnTypes() {
      return Collections.unmodifiableList(idColumnTypes.get());
   }

   public String tableNamePrefix() {
      return tableNamePrefix.get();
   }

   public String cacheName() {
      return cacheName.get();
   }

   public List<String> dataColumnNames() {
      return Collections.unmodifiableList(dataColumnNames.get());
   }

   public List<String> dataColumnTypes() {
      return Collections.unmodifiableList(dataColumnTypes.get());
   }

   public String timestampColumnName() {
      return timestampColumnName.get();
   }

   public String timestampColumnType() {
      return timestampColumnType.get();
   }

   public int fetchSize() {
      return fetchSize.get();
   }

   /**
    * @return the size of batches to process.  Guaranteed to be a power of two.
    */
   public int batchSize() {
      return batchSize.get();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "TableManipulationConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      TableManipulationConfiguration other = (TableManipulationConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }
}