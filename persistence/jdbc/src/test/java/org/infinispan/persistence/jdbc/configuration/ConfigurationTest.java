package org.infinispan.persistence.jdbc.configuration;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Properties;

import org.h2.Driver;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.jdbc.configuration.ConfigurationTest")
public class ConfigurationTest {

   private static final String JDBC_URL = "jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1";

   public void testImplicitPooledConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcBinaryStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL);
      Configuration configuration = b.build();
      JdbcBinaryStoreConfiguration store = (JdbcBinaryStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
   }

   public void testImplicitManagedConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcBinaryStoreConfigurationBuilder.class)
         .dataSource().jndiUrl("java:jboss/datasources/ExampleDS");
      Configuration configuration = b.build();
      JdbcBinaryStoreConfiguration store = (JdbcBinaryStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof ManagedConnectionFactoryConfiguration;
   }

   public void testJdbcBinaryCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcBinaryStoreConfigurationBuilder.class)
         .connectionPool()
            .connectionUrl(JDBC_URL)
            .username("dbuser")
            .password("dbpass")
            .driverClass(Driver.class)
         .fetchPersistentState(true)
         .concurrencyLevel(32)
         .table()
            .tableNamePrefix("BINARY_")
            .idColumnNames("id").idColumnTypes("VARCHAR")
            .dataColumnNames("datum").dataColumnTypes("BINARY")
            .timestampColumnName("version").timestampColumnType("BIGINT")
         .async().enable();
      Configuration configuration = b.build();
      JdbcBinaryStoreConfiguration store = (JdbcBinaryStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store.table().tableNamePrefix().equals("BINARY_");
      assert store.table().idColumnNames().equals(Arrays.asList("id"));
      assert store.table().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store.table().dataColumnNames().equals(Arrays.asList("datum"));
      assert store.table().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store.table().timestampColumnName().equals("version");
      assert store.table().timestampColumnType().equals("BIGINT");
      assert store.fetchPersistentState();
      assert store.lockConcurrencyLevel() == 32;
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcBinaryStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcBinaryStoreConfiguration store2 = (JdbcBinaryStoreConfiguration) configuration2.persistence().stores().get(0);
      assert store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store2.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store2.table().tableNamePrefix().equals("BINARY_");
      assert store2.table().idColumnNames().equals(Arrays.asList("id"));
      assert store2.table().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store2.table().dataColumnNames().equals(Arrays.asList("datum"));
      assert store2.table().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store2.table().timestampColumnName().equals("version");
      assert store2.table().timestampColumnType().equals("BIGINT");
      assert store2.fetchPersistentState();
      assert store2.lockConcurrencyLevel() == 32;
      assert store2.async().enabled();
   }

   public void testJdbcMixedCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      JdbcMixedStoreConfigurationBuilder mixedBuilder = b.persistence().addStore(JdbcMixedStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL)
         .fetchPersistentState(true)
         .lockConcurrencyLevel(32)
         .fetchSize(50)
         .batchSize(50)
         .dialect(DatabaseType.H2)
         .dbMajorVersion(1)
         .dbMinorVersion(4);
      mixedBuilder.async().enable();

      mixedBuilder.binaryTable()
         .tableNamePrefix("BINARY_")
         .idColumnNames("id").idColumnTypes("VARCHAR")
         .dataColumnNames("datum").dataColumnTypes("BINARY")
         .timestampColumnName("version").timestampColumnType("BIGINT");

      mixedBuilder.stringTable()
         .tableNamePrefix("STRINGS_")
         .idColumnNames("id").idColumnTypes("VARCHAR")
         .dataColumnNames("datum").dataColumnTypes("BINARY")
         .timestampColumnName("version").timestampColumnType("BIGINT");

      Configuration configuration = b.build();
      JdbcMixedStoreConfiguration store = (JdbcMixedStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store.binaryTable().tableNamePrefix().equals("BINARY_");
      assert store.binaryTable().idColumnNames().equals(Arrays.asList("id"));
      assert store.binaryTable().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store.binaryTable().dataColumnNames().equals(Arrays.asList("datum"));
      assert store.binaryTable().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store.binaryTable().timestampColumnName().equals("version");
      assert store.binaryTable().timestampColumnType().equals("BIGINT");
      assert store.stringTable().tableNamePrefix().equals("STRINGS_");
      assert store.stringTable().idColumnNames().equals(Arrays.asList("id"));
      assert store.stringTable().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store.stringTable().dataColumnNames().equals(Arrays.asList("datum"));
      assert store.stringTable().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store.stringTable().timestampColumnName().equals("version");
      assert store.stringTable().timestampColumnType().equals("BIGINT");
      assert store.batchSize() == 50;
      assert store.fetchSize() == 50;
      assert store.dialect() == DatabaseType.H2;
      assert store.dbMajorVersion() == 1;
      assert store.dbMinorVersion() == 4;
      assert store.fetchPersistentState();
      assert store.lockConcurrencyLevel() == 32;
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcMixedStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcMixedStoreConfiguration store2 = (JdbcMixedStoreConfiguration) configuration2.persistence().stores().get(0);
      assert store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store2.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store2.binaryTable().idColumnNames().equals(Arrays.asList("id"));
      assert store2.binaryTable().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store2.binaryTable().dataColumnNames().equals(Arrays.asList("datum"));
      assert store2.binaryTable().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store2.binaryTable().timestampColumnName().equals("version");
      assert store2.binaryTable().timestampColumnType().equals("BIGINT");
      assert store2.stringTable().tableNamePrefix().equals("STRINGS_");
      assert store2.stringTable().idColumnNames().equals(Arrays.asList("id"));
      assert store2.stringTable().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store2.stringTable().dataColumnNames().equals(Arrays.asList("datum"));
      assert store2.stringTable().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store2.stringTable().timestampColumnName().equals("version");
      assert store2.stringTable().timestampColumnType().equals("BIGINT");
      assert store2.fetchPersistentState();
      assert store2.lockConcurrencyLevel() == 32;
      assert store2.async().enabled();
   }

   public void testJdbcStringCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .connectionPool()
            .connectionUrl(JDBC_URL)
         .fetchPersistentState(true)
         .table()
            .tableNamePrefix("STRINGS_")
            .idColumnNames("id").idColumnTypes("VARCHAR")
            .dataColumnNames("datum").dataColumnTypes("BINARY")
            .timestampColumnName("version").timestampColumnType("BIGINT")
         .async().enable();
      Configuration configuration = b.build();
      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store.table().tableNamePrefix().equals("STRINGS_");
      assert store.table().idColumnNames().equals(Arrays.asList("id"));
      assert store.table().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store.table().dataColumnNames().equals(Arrays.asList("datum"));
      assert store.table().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store.table().timestampColumnName().equals("version");
      assert store.table().timestampColumnType().equals("BIGINT");
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcStringBasedStoreConfiguration store2 = (JdbcStringBasedStoreConfiguration) configuration2.persistence().stores().get(0);
      assert store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store2.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store2.table().tableNamePrefix().equals("STRINGS_");
      assert store2.table().idColumnNames().equals(Arrays.asList("id"));
      assert store2.table().idColumnTypes().equals(Arrays.asList("VARCHAR"));
      assert store2.table().dataColumnNames().equals(Arrays.asList("datum"));
      assert store2.table().dataColumnTypes().equals(Arrays.asList("BINARY"));
      assert store2.table().timestampColumnName().equals("version");
      assert store2.table().timestampColumnType().equals("BIGINT");
      assert store2.fetchPersistentState();
      assert store2.async().enabled();
   }

   public void testTableProperties() {
      Properties props = new Properties();
      props.put("createOnStart", "false");
      props.put("dropOnExit", "true");

      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL)
         .withProperties(props);
      Configuration stringConfiguration = b.build();

      JdbcStringBasedStoreConfiguration stringStoreConfiguration = (JdbcStringBasedStoreConfiguration) stringConfiguration.persistence().stores().get(0);
      verifyTableManipulation(stringStoreConfiguration.table());

      b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcBinaryStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL)
         .withProperties(props);

      Configuration binaryConfiguration = b.build();

      JdbcBinaryStoreConfiguration binaryStoreConfiguration = (JdbcBinaryStoreConfiguration) binaryConfiguration.persistence().stores().get(0);
      verifyTableManipulation(binaryStoreConfiguration.table());

      b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcMixedStoreConfigurationBuilder.class)
         .stringTable().tableNamePrefix("STRINGS_")
         .binaryTable().tableNamePrefix("BINARY_")
         .connectionPool().connectionUrl(JDBC_URL)
         .withProperties(props);
      Configuration mixedConfiguration = b.build();

      JdbcMixedStoreConfiguration mixedStoreConfiguration = (JdbcMixedStoreConfiguration) mixedConfiguration.persistence().stores().get(0);
      verifyTableManipulation(mixedStoreConfiguration.binaryTable());
      verifyTableManipulation(mixedStoreConfiguration.stringTable());
   }

   private void verifyTableManipulation(TableManipulationConfiguration table) {
      assertFalse(table.createOnStart());
      assertTrue(table.dropOnExit());
   }

}
