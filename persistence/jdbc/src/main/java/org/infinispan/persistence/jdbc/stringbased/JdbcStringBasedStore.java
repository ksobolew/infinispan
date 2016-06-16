package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.persistence.PersistenceUtil.getExpiryTime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.jdbc.table.management.TableManagerFactory;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.InitializationContextAware;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link org.infinispan.persistence.spi.AdvancedCacheLoader} implementation that stores the entries in a database. In contrast to the
 * {@link org.infinispan.persistence.jdbc.binary.JdbcBinaryStore}, this cache store will store each entry within a row
 * in the table (rather than grouping multiple entries into an row). This assures a finer grained granularity for all
 * operation, and better performance. In order to be able to store non-string keys, it relies on an {@link
 * org.infinispan.persistence.keymappers.Key2StringMapper}.
 * <p/>
 * Note that only the keys are stored as strings, the values are still saved as binary data. Using a character
 * data type for the value column will result in unmarshalling errors.
 * <p/>
 * The actual storage table is defined through configuration {@link org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}. The table can
 * be
 * created/dropped on-the-fly, at deployment time. For more details consult javadoc for {@link
 * org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration}.
 * <p/>
 * It is recommended to use {@link JdbcStringBasedStore}} over
 * {@link org.infinispan.persistence.jdbc.binary.JdbcBinaryStore}} whenever it is possible, as is has a better performance.
 * One scenario in which this is not possible to use it though, is when you can't write an {@link org.infinispan.persistence.keymappers.Key2StringMapper}} to map the
 * keys to to string objects (e.g. when you don't have control over the types of the keys, for whatever reason).
 * <p/>
 * <b>Preload</b>.In order to support preload functionality the store needs to read the string keys from the database and transform them
 * into the corresponding key objects. {@link org.infinispan.persistence.keymappers.Key2StringMapper} only supports
 * key to string transformation(one way); in order to be able to use preload one needs to specify an
 * {@link org.infinispan.persistence.keymappers.TwoWayKey2StringMapper}, which extends {@link org.infinispan.persistence.keymappers.Key2StringMapper} and
 * allows bidirectional transformation.
 * <p/>
 * <b>Rehashing</b>. When a node leaves/joins, Infinispan moves around persistent state as part of rehashing process.
 * For this it needs access to the underlaying key objects, so if distribution is used, the mapper needs to be an
 * {@link org.infinispan.persistence.keymappers.TwoWayKey2StringMapper} otherwise the cache won't start (same constraint as with preloading).
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.persistence.keymappers.Key2StringMapper
 * @see org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper
 */
@ConfiguredBy(JdbcStringBasedStoreConfiguration.class)
public class JdbcStringBasedStore<K, V> implements AdvancedLoadWriteStore<K, V> {

   private static final Log log = LogFactory.getLog(JdbcStringBasedStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private JdbcStringBasedStoreConfiguration configuration;
   private List<String> keyColumnNames;
   private List<String> valueColumnNames;
   private String timestampColumnName;

   private Key2StringMapper key2StringMapper;
   private Key2StringMapper value2StringMapper;
   private ConnectionFactory connectionFactory;
   private TableManager tableManager;
   private InitializationContext ctx;
   private String cacheName;
   private GlobalConfiguration globalConfiguration;


   @Override
   public void init(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      this.keyColumnNames = this.configuration.table().idColumnNames();
      this.valueColumnNames = this.configuration.table().dataColumnNames();
      this.timestampColumnName = this.configuration.table().timestampColumnName();
      this.ctx = ctx;
      cacheName = ctx.getCache().getName();
      globalConfiguration = ctx.getCache().getCacheManager().getCacheManagerConfiguration();
   }

   @Override
   public void start() {
      if (configuration.manageConnectionFactory()) {
         ConnectionFactory factory = ConnectionFactory.getConnectionFactory(configuration.connectionFactory().connectionFactoryClass());
         factory.start(configuration.connectionFactory(), factory.getClass().getClassLoader());
         initializeConnectionFactory(factory);
      }
      try {
         Object keyMapper = Util.loadClassStrict(configuration.key2StringMapper(),
                                              globalConfiguration.classLoader()).newInstance();
         if (keyMapper instanceof Key2StringMapper) key2StringMapper = (Key2StringMapper) keyMapper;
         Object valueMapper = Util.loadClassStrict(configuration.value2StringMapper(),
               globalConfiguration.classLoader()).newInstance();
         if (valueMapper instanceof Key2StringMapper) {
            value2StringMapper = (Key2StringMapper) valueMapper;
            if (value2StringMapper instanceof InitializationContextAware) {
               ((InitializationContextAware) value2StringMapper).setInitializationContext(ctx);
            }
         }
      } catch (Exception e) {
         log.errorf("Trying to instantiate %s, however it failed due to %s", configuration.key2StringMapper(),
                    e.getClass().getName());
         throw new IllegalStateException("This should not happen.", e);
      }
      if (trace) {
         log.tracef("Using key2StringMapper: %s", key2StringMapper.getClass().getName());
      }
      if (configuration.preload()) {
         enforceTwoWayMapper("preload");
      }
      if (isDistributed()) {
         enforceTwoWayMapper("distribution/rehashing");
      }
   }

   @Override
   public void stop() {
      Throwable cause = null;
      try {
         tableManager.stop();
      } catch (Throwable t) {
         cause = t.getCause();
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }

      try {
         if (configuration.connectionFactory() instanceof ManagedConnectionFactory) {
            log.tracef("Stopping mananged connection factory: %s", connectionFactory);
            connectionFactory.stop();
         }
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      if (cause != null) {
         throw new PersistenceException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   public void write(MarshalledEntry<? extends K, ? extends V> entry) {
      Connection connection = null;
      Object[] keyObjs = key2Objs(entry.getKey());
      try {
         connection = connectionFactory.getConnection();
         if (tableManager.isUpsertSupported()) {
            executeUpsert(connection, entry, keyObjs);
         } else {
            executeLegacyUpdate(connection, entry, keyObjs);
         }
      } catch (SQLException ex) {
         log.sqlFailureStoringKey(Arrays.toString(keyObjs), ex);
         throw new PersistenceException(String.format("Error while storing string key to database; key: '%s'", Arrays.asList(keyObjs)), ex);
      } finally {
         connectionFactory.releaseConnection(connection);
      }
   }

   private void executeUpsert(Connection connection, MarshalledEntry<? extends K, ? extends V> entry, Object[] keyObjs)
         throws SQLException {
      PreparedStatement ps = null;
      String sql = tableManager.getUpsertRowSql();
      if (trace) {
         log.tracef("Running sql '%s'. Key string is '%s'", sql, Arrays.asList(keyObjs));
      } try {
         ps = connection.prepareStatement(sql);
         prepareUpdateStatement(entry, keyObjs, ps);
         ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   private void executeLegacyUpdate(Connection connection, MarshalledEntry<? extends K, ? extends V> entry, Object[] keyObjs)
         throws SQLException {
      String sql = tableManager.getSelectIdRowSql();
      if (trace) {
         log.tracef("Running sql '%s'. Key string is '%s'", sql, Arrays.asList(keyObjs));
      }
      PreparedStatement ps = null;
      try {
         ps = connection.prepareStatement(sql);
         for (int i = 0; i < keyColumnNames.size(); i++) {
            ps.setObject(1 + i, keyObjs[i]);
         }
         ResultSet rs = ps.executeQuery();
         if (rs.next()) {
            sql = tableManager.getUpdateRowSql();
         } else {
            sql = tableManager.getInsertRowSql();
         }
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         if (trace) {
            log.tracef("Running sql '%s'. Key string is '%s'", sql, Arrays.asList(keyObjs));
         }
         ps = connection.prepareStatement(sql);
         prepareUpdateStatement(entry, keyObjs, ps);
         ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   @SuppressWarnings("unchecked")
   @Override
   public MarshalledEntry<K, V> load(Object key) {
      Object[] lockingKey = key2Objs(key);
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      MarshalledEntry<K, V> storedValue = null;
      try {
         String sql = tableManager.getSelectRowSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         for (int i = 0; i < keyColumnNames.size(); i++) {
            ps.setObject(1 + i, lockingKey[i]);
         }
         rs = ps.executeQuery();
         if (rs.next()) {
            Object[] valueObjs = new Object[valueColumnNames.size() + 1];
            valueObjs[0] = key;
            int j = keyColumnNames.size() + 1;
            for (int i = 0; i < valueColumnNames.size(); i++) {
               valueObjs[i + 1] = rs.getObject(j++);
            }
            storedValue = (MarshalledEntry<K, V>) ((TwoWayKey2StringMapper) value2StringMapper).getKeyMapping(valueObjs);
         }
      } catch (SQLException e) {
         log.sqlFailureReadingKey(key, Arrays.toString(lockingKey), e);
         throw new PersistenceException(String.format(
               "SQL error while fetching stored entry with key: %s, lockingKey: %s",
               key, Arrays.asList(lockingKey)), e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
      if (storedValue != null && storedValue.getMetadata() != null &&
            storedValue.getMetadata().isExpired(ctx.getTimeService().wallClockTime())) {
         return null;
      }
      return storedValue;
   }

   @Override
   public boolean delete(Object key) {
      Connection connection = null;
      PreparedStatement ps = null;
      Object[] keyObjs = key2Objs(key);
      try {
         String sql = tableManager.getDeleteRowSql();
         if (trace) {
            log.tracef("Running sql '%s' on %s", sql, Arrays.asList(keyObjs));
         }
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         for (int i = 0; i < keyColumnNames.size(); i++) {
            ps.setObject(1 + i, keyObjs[i]);
         }
         return ps.executeUpdate() == 1;
      } catch (SQLException ex) {
         log.sqlFailureRemovingKeys(ex);
         throw new PersistenceException("Error while removing string keys from database", ex);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
   public void clear() throws PersistenceException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManager.getDeleteAllRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         int result = ps.executeUpdate();
         if (trace) {
            log.tracef("Successfully removed %d rows.", result);
         }
      } catch (SQLException ex) {
         log.failedClearingJdbcCacheStore(ex);
         throw new PersistenceException("Failed clearing cache store", ex);
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   @Override
   public void purge(Executor executor, PurgeListener<? super K> task) {
      //todo we should make the notification to the purge listener here
      ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(executor);
      Future<Void> future = ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Connection conn = null;
            PreparedStatement ps = null;
            try {
               String sql = tableManager.getDeleteExpiredRowsSql();
               conn = connectionFactory.getConnection();
               ps = conn.prepareStatement(sql);
               ps.setLong(1, ctx.getTimeService().wallClockTime());
               int result = ps.executeUpdate();
               if (trace) {
                  log.tracef("Successfully purged %d rows.", result);
               }
            } catch (SQLException ex) {
               log.failedClearingJdbcCacheStore(ex);
               throw new PersistenceException("Failed clearing string based JDBC store", ex);
            } finally {
               JdbcUtil.safeClose(ps);
               connectionFactory.releaseConnection(conn);
            }
            return null;
         }
      });
      try {
         future.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         log.errorExecutingParallelStoreTask(e);
         throw new PersistenceException(e);
      }
   }

   @Override
   public boolean contains(Object key) {
      //we can do better if needed...
      return load(key) != null;
   }


   @Override
   public void process(final KeyFilter<? super K> filter, final CacheLoaderTask<K, V> task, Executor executor, final boolean fetchValue, final boolean fetchMetadata) {

      ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(executor);
      Future<Void> future = ecs.submit(new Callable<Void>() {
         @SuppressWarnings("unchecked")
         @Override
         public Void call() throws Exception {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
               String sql = tableManager.getLoadNonExpiredAllRowsSql();
               if (trace) {
                  log.tracef("Running sql %s", sql);
               }
               conn = connectionFactory.getConnection();
               ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
               ps.setLong(1, ctx.getTimeService().wallClockTime());
               ps.setFetchSize(tableManager.getFetchSize());
               rs = ps.executeQuery();

               TaskContext taskContext = new TaskContextImpl();
               while (rs.next()) {
                  Object[] keyObjs = new Object[keyColumnNames.size()];
                  int j = 1;
                  Object[] valueObjs = new Object[valueColumnNames.size() + 1];
                  for (int i = 0; i < valueColumnNames.size(); i++) {
                     valueObjs[i + 1] = rs.getObject(j++);
                  }
                  for (int i = 0; i < keyObjs.length; i++) {
                     keyObjs[i] = rs.getObject(j++);
                  }
                  K key = (K) ((TwoWayKey2StringMapper) key2StringMapper).getKeyMapping(keyObjs);
                  valueObjs[0] = key;
                  if (taskContext.isStopped()) break;
                  if (filter != null && !filter.accept(key))
                     continue;
                  MarshalledEntry<K, V> entry = (MarshalledEntry<K, V>)
                        ((TwoWayKey2StringMapper) value2StringMapper).getKeyMapping(valueObjs);
                  task.processEntry(entry, taskContext);
               }
               return null;
            } catch (SQLException e) {
               log.sqlFailureFetchingAllStoredEntries(e);
               throw new PersistenceException("SQL error while fetching all StoredEntries", e);
            } finally {
               JdbcUtil.safeClose(rs);
               JdbcUtil.safeClose(ps);
               connectionFactory.releaseConnection(conn);
            }
         }
      });
      try {
         future.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         log.errorExecutingParallelStoreTask(e);
         throw new PersistenceException(e);
      }
   }

   @Override
   public int size() {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManager.getCountRowsSql();
         ps = conn.prepareStatement(sql);
         rs = ps.executeQuery();
         rs.next();
         return rs.getInt(1);
      } catch (SQLException e) {
         log.sqlFailureIntegratingState(e);
         throw new PersistenceException("SQL failure while integrating state into store", e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   private void prepareUpdateStatement(MarshalledEntry<? extends K, ? extends V> entry, Object[] key, PreparedStatement ps) throws SQLException {
      Object[] valueObjs = value2StringMapper.getObjectsMapping(entry);
      int i = 1;
      for (int j = 0; j < valueColumnNames.size(); j++) {
         if (!valueColumnNames.get(j).equals(timestampColumnName)) {
            ps.setObject(i++, valueObjs[j]);
         }
      }
      ps.setLong(i++, getExpiryTime(entry.getMetadata()));
      for (int j = 0; j < keyColumnNames.size(); j++) {
         ps.setObject(i++, key[j]);
      }
   }

   private Object[] key2Objs(Object key) throws PersistenceException {
      if (!key2StringMapper.isSupportedType(key.getClass())) {
         throw new UnsupportedKeyTypeException(key);
      }
      return key2StringMapper.getObjectsMapping(key);
   }

   public boolean supportsKey(Class<?> keyType) {
      return key2StringMapper.isSupportedType(keyType);
   }

   /**
    * Keeps a reference to the connection factory for further use. Also initializes the {@link
    * TableManager} that needs connections. This method should be called when you don't
    * want the store to manage the connection factory, perhaps because it is using an shared connection factory: see
    * {@link org.infinispan.persistence.jdbc.mixed.JdbcMixedStore} for such an example of this.
    */
   public void initializeConnectionFactory(ConnectionFactory connectionFactory) throws PersistenceException {
      this.connectionFactory = connectionFactory;
      tableManager = TableManagerFactory.getManager(connectionFactory, configuration);
      tableManager.setCacheName(cacheName);
      tableManager.start();
   }

   public ConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   public TableManager getTableManager() {
      return tableManager;
   }

   private void enforceTwoWayMapper(String where) throws PersistenceException {
      if (!(key2StringMapper instanceof TwoWayKey2StringMapper)) {
         log.invalidKey2StringMapper(where, key2StringMapper.getClass().getName());
         throw new PersistenceException(String.format("Invalid key to string mapper : %s", key2StringMapper.getClass().getName()));
      }
   }

   public boolean isDistributed() {
      return ctx.getCache().getCacheConfiguration() != null && ctx.getCache().getCacheConfiguration().clustering().cacheMode().isDistributed();
   }
}